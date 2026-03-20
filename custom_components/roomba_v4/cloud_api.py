from __future__ import annotations

from datetime import UTC, datetime, timedelta
import asyncio
import copy
import base64
import hashlib
import hmac
import json
from json.decoder import JSONDecodeError
import logging
import ssl
import urllib.parse
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import websockets
from websockets.datastructures import Headers

from .const import API_USER_AGENT, AWS_USER_AGENT

_LOGGER = logging.getLogger(__name__)

DISCOVERY_GB_URL = "https://disc-prod.iot.irobotapi.com/v1/discover/endpoints?country_code=GB"
GIGYA_ACCOUNT_INFO_URL = "https://accounts.us1.gigya.com/accounts.getAccountInfo"
API_GATEWAY_HOST_OVERRIDES = {
    "unauth2.prod.iot.irobotapi.com": {
        "host": "yc82ntqag9.execute-api.us-east-1.amazonaws.com",
        "stage_prefix": "/dev",
    },
    "auth2.prod.iot.irobotapi.com": {
        "host": "qqzyyfdzbf.execute-api.us-east-1.amazonaws.com",
        "stage_prefix": "/dev",
    },
}


class CloudApiError(Exception):
    pass


class AuthenticationError(CloudApiError):
    pass


class AWSSignatureV4:
    def __init__(self, access_key_id: str, secret_access_key: str, session_token: str | None = None) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token

    def _hmac_sha256(self, key: bytes, data: str) -> bytes:
        return hmac.new(key, data.encode("utf-8"), hashlib.sha256).digest()

    def _sha256_hex(self, data: str) -> str:
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _get_signature_key(self, date_stamp: str, region: str, service: str) -> bytes:
        k_date = self._hmac_sha256(f"AWS4{self.secret_access_key}".encode(), date_stamp)
        k_region = self._hmac_sha256(k_date, region)
        k_service = self._hmac_sha256(k_region, service)
        return self._hmac_sha256(k_service, "aws4_request")

    def generate_signed_headers(
        self,
        *,
        method: str,
        service: str,
        region: str,
        host: str,
        path: str,
        query_params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        payload: str = "",
    ) -> dict[str, str]:
        query_params = query_params or {}
        headers = headers or {}

        now = datetime.now(tz=UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        canonical_uri = urllib.parse.quote(path, safe="/")
        canonical_query_string = "&".join(
            f"{urllib.parse.quote(str(k), safe='~')}={urllib.parse.quote(str(v), safe='~')}"
            for k, v in sorted(query_params.items())
        )

        merged_headers = {"host": host, "x-amz-date": amz_date, **headers}
        canonical_source = {k.lower(): " ".join(str(v).strip().split()) for k, v in merged_headers.items()}
        signed_header_keys = sorted(canonical_source)
        canonical_headers = "".join(f"{k}:{canonical_source[k]}\n" for k in signed_header_keys)
        signed_headers = ";".join(signed_header_keys)

        payload_hash = self._sha256_hex(payload)
        canonical_request = (
            f"{method.upper()}\n"
            f"{canonical_uri}\n"
            f"{canonical_query_string}\n"
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        string_to_sign = (
            f"{algorithm}\n"
            f"{amz_date}\n"
            f"{credential_scope}\n"
            f"{self._sha256_hex(canonical_request)}"
        )
        signing_key = self._get_signature_key(date_stamp, region, service)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        final_headers = dict(merged_headers)
        final_headers["Authorization"] = (
            f"{algorithm} Credential={self.access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        if self.session_token:
            final_headers["x-amz-security-token"] = self.session_token
        return final_headers


class IRobotCloudApi:
    def __init__(self, *, username: str, password: str, country_code: str = "US", session: aiohttp.ClientSession) -> None:
        self.username = username
        self.password = password
        self.country_code = country_code
        self.session = session
        self.config = {"appId": str(uuid.uuid4()), "deviceId": str(uuid.uuid4())}
        self.uid = None
        self.uid_signature = None
        self.signature_timestamp = None
        self.credentials: dict[str, Any] | None = None
        self.deployment: dict[str, Any] | None = None
        self.robots: dict[str, dict[str, Any]] = {}
        self.last_login_response: dict[str, Any] | None = None
        self.gigya_account_info: dict[str, Any] | None = None
        self.discovered_endpoints: dict[str, Any] | None = None
        self.form_headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": API_USER_AGENT}
        self._ssl_context: ssl.SSLContext | None = None
        self.debug_dir: Path | None = None
        self._subscriber_task: asyncio.Task | None = None
        self._subscriber_stop = asyncio.Event()
        self._subscriber_cycle = 0
        self._live_state: dict[str, Any] = {}
        self._live_state_sources: dict[str, dict[str, Any]] = {
            "currentstate_update": {},
            "currentstate_get": {},
            "livemap": {},
            "stats": {},
            "shadow": {},
        }
        self._live_state_listeners: list[Any] = []
        self._last_live_state_update: str | None = None
        self.local_mqtt_ip_override: str | None = None
        self.local_mqtt_port: int = 8883
        self._mqtt_slot_blocked_until: datetime | None = None
        self._single_slot_safe_mode: bool = True
        self._subscriber_ws: Any | None = None
        self._subscriber_robot_id: str | None = None
        self._subscriber_send_lock = asyncio.Lock()
        self._command_send_lock = asyncio.Lock()
        self._subscriber_ready = asyncio.Event()
        self._subscriber_topics_subscribed: set[str] = set()
        self._livemap_topics: dict[str, str] = {}
        self.hass = None

    async def discover_endpoints(self) -> dict[str, Any]:
        urls: list[str] = [DISCOVERY_GB_URL]
        configured = f"https://disc-prod.iot.irobotapi.com/v1/discover/endpoints?country_code={urllib.parse.quote(self.country_code)}"
        if configured not in urls:
            urls.append(configured)

        last_error: str | None = None
        for url in urls:
            async with self.session.get(url, headers={"User-Agent": API_USER_AGENT}) as resp:
                txt = await resp.text()
                if resp.status != 200:
                    last_error = f"{url} -> ({resp.status}) {txt[:400]}"
                    continue
                data = json.loads(txt)
                deployments = data.get("deployments", {})
                current = data.get("current_deployment")
                self.deployment = deployments.get(current, {})
                self.discovered_endpoints = data
                data["_discovery_url_used"] = url
                return data

        raise CloudApiError(f"Endpoint discovery failed: {last_error or 'unknown error'}")

    async def get_gigya_account_info(self, api_key: str) -> dict[str, Any]:
        if not self.uid or not self.uid_signature or not self.signature_timestamp:
            raise AuthenticationError("Gigya identity is not established")

        payload = {
            "UID": self.uid,
            "UIDSignature": self.uid_signature,
            "signatureTimestamp": self.signature_timestamp,
            "include": "profile,data,emails,subscriptions,preferences",
            "apikey": api_key,
            "sdk": "ios_swift_1.3.0",
            "targetEnv": "mobile",
        }
        async with self.session.post(
            GIGYA_ACCOUNT_INFO_URL,
            headers=self.form_headers,
            data=urllib.parse.urlencode(payload),
        ) as resp:
            txt = await resp.text()
            try:
                data = json.loads(txt)
            except JSONDecodeError as err:
                raise AuthenticationError(f"Invalid Gigya account info JSON: {txt[:400]}") from err
        if data.get("errorCode", 0) != 0:
            raise AuthenticationError(f"Gigya getAccountInfo failed: {data}")
        self.gigya_account_info = data
        return data

    async def login_gigya(self, api_key: str) -> dict[str, Any]:
        endpoints = await self.discover_endpoints()
        gigya = endpoints.get("gigya") or self.deployment.get("gigya")
        if not gigya:
            raise AuthenticationError(f"Discovery response missing gigya config: keys={list(endpoints.keys())}")
        datacenter = gigya.get('datacenter_domain') or 'us1.gigya.com'
        base_acc = f"https://accounts.{datacenter}/accounts."
        payload = {
            "loginMode": "standard",
            "loginID": self.username,
            "password": self.password,
            "include": "profile,data,emails,subscriptions,preferences,",
            "includeUserInfo": "true",
            "targetEnv": "mobile",
            "source": "showScreenSet",
            "sdk": "ios_swift_1.3.0",
            "sessionExpiration": "-2",
            "apikey": api_key,
        }
        async with self.session.post(f"{base_acc}login", headers=self.form_headers, data=urllib.parse.urlencode(payload)) as resp:
            txt = await resp.text()
            try:
                data = json.loads(txt)
            except JSONDecodeError as err:
                raise AuthenticationError(f"Invalid Gigya JSON: {txt[:400]}") from err
        if data.get("errorCode", 0) != 0:
            raise AuthenticationError(f"Gigya login failed: {data}")
        self.uid = data["UID"]
        self.uid_signature = data["UIDSignature"]
        self.signature_timestamp = data["signatureTimestamp"]
        try:
            await self.get_gigya_account_info(api_key)
        except Exception as err:
            _LOGGER.debug("Gigya getAccountInfo probe failed: %s", err)
        return data

    async def login_irobot(self) -> dict[str, Any]:
        if not self.deployment:
            await self.discover_endpoints()
        payload = {
            "app_id": f"IOS-{self.config['appId']}",
            "app_info": {
                "device_id": f"IOS-{self.config['deviceId']}",
                "device_name": "Home Assistant",
                "language": "en_US",
                "version": "7.16.2",
            },
            "assume_robot_ownership": "0",
            "authorizer_params": {"devices_per_token": 5},
            "gigya": {
                "signature": self.uid_signature,
                "timestamp": self.signature_timestamp,
                "uid": self.uid,
            },
            "multiple_authorizer_token_support": True,
            "skip_ownership_check": "0",
        }
        async with self.session.post(
            f"{self.deployment['httpBase']}/v2/login",
            headers={"Content-Type": "application/json", "User-Agent": API_USER_AGENT},
            json=payload,
        ) as resp:
            txt = await resp.text()
            try:
                data = json.loads(txt)
            except JSONDecodeError as err:
                raise AuthenticationError(f"Invalid iRobot login JSON: {txt[:400]}") from err
        if data.get("errorCode"):
            message = str(data.get("errorMessage") or data)
            if "No mqtt slot available" in message:
                self._mqtt_slot_blocked_until = datetime.now(tz=UTC) + timedelta(minutes=5)
                try:
                    await self._write_runtime_debug("mqtt_slot_blocked", {
                        "ts": datetime.now(tz=UTC).isoformat(),
                        "blocked_until": self._mqtt_slot_blocked_until.isoformat(),
                        "message": message,
                    })
                except Exception:
                    pass
            raise AuthenticationError(message)
        if "credentials" not in data or "robots" not in data:
            message = f"Incomplete iRobot login response: {data}"
            if "No mqtt slot available" in message:
                self._mqtt_slot_blocked_until = datetime.now(tz=UTC) + timedelta(minutes=5)
                try:
                    await self._write_runtime_debug("mqtt_slot_blocked", {
                        "ts": datetime.now(tz=UTC).isoformat(),
                        "blocked_until": self._mqtt_slot_blocked_until.isoformat(),
                        "message": message,
                    })
                except Exception:
                    pass
            raise AuthenticationError(message)
        self.credentials = data["credentials"]
        self.robots = data["robots"]
        self.last_login_response = data
        return data

    async def authenticate(self) -> dict[str, Any]:
        if self._mqtt_slot_blocked_until and datetime.now(tz=UTC) < self._mqtt_slot_blocked_until:
            raise AuthenticationError(f"No mqtt slot available; cooling down until {self._mqtt_slot_blocked_until.isoformat()}")
        endpoints = await self.discover_endpoints()
        gigya = endpoints.get("gigya") or self.deployment.get("gigya")
        if not gigya:
            raise AuthenticationError(f"Discovery response does not contain gigya config. Keys: {list(endpoints.keys())}")
        await self.login_gigya(gigya["api_key"])
        return await self.login_irobot()

    def _resolve_execute_api_target(self, url: str) -> tuple[str, str, str]:
        parsed = urllib.parse.urlparse(url)
        override = API_GATEWAY_HOST_OVERRIDES.get(parsed.netloc)
        if not override:
            return parsed.netloc, parsed.path or "/", url

        stage_prefix = str(override.get("stage_prefix") or "")
        original_path = parsed.path or "/"
        signing_path = f"{stage_prefix}{original_path}" if stage_prefix else original_path
        final_url = urllib.parse.urlunparse((
            parsed.scheme or "https",
            str(override["host"]),
            signing_path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        ))
        return str(override["host"]), signing_path, final_url

    async def _aws_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        retry: bool = True,
        accept: str = "application/json",
        method: str = "GET",
        payload: str = "",
    ) -> Any:
        if not self.credentials:
            raise AuthenticationError("Not authenticated")
        region = self.credentials["CognitoId"].split(":")[0]
        parsed = urllib.parse.urlparse(url)
        signing_host, signing_path, final_url = self._resolve_execute_api_target(url)
        signer = AWSSignatureV4(
            access_key_id=self.credentials["AccessKeyId"],
            secret_access_key=self.credentials["SecretKey"],
            session_token=self.credentials["SessionToken"],
        )
        headers = signer.generate_signed_headers(
            method=method,
            service="execute-api",
            region=region,
            host=signing_host,
            path=signing_path,
            query_params=params or {},
            headers={"accept": accept, "content-type": "application/json", "user-agent": AWS_USER_AGENT},
            payload=payload,
        )
        final_url = final_url if not params else f"{final_url}?{urllib.parse.urlencode(params)}"
        request_coro = self.session.get if method.upper() == "GET" else self.session.post
        kwargs = {"headers": headers}
        if method.upper() != "GET":
            kwargs["data"] = payload.encode("utf-8")
        try:
            async with request_coro(final_url, **kwargs) as resp:
                txt = await resp.text()
                if resp.status == 403 and retry:
                    _LOGGER.debug("roomba_v4 AWS request got 403, re-authenticating: method=%s url=%s", method, url)
                    await self.authenticate()
                    return await self._aws_request(url, params, retry=False, accept=accept, method=method, payload=payload)
                if resp.status != 200:
                    _LOGGER.warning("roomba_v4 AWS request failed: method=%s status=%s url=%s body=%s", method, resp.status, url, txt[:500])
                    raise CloudApiError(f"AWS request failed ({resp.status}) {url}: {txt[:500]}")
                if not txt:
                    return {}
                try:
                    return json.loads(txt)
                except JSONDecodeError:
                    return txt
        except asyncio.TimeoutError as err:
            _LOGGER.debug("roomba_v4 AWS request timed out: method=%s url=%s", method, url)
            raise CloudApiError(f"AWS request timed out for {url}") from err
        except aiohttp.ClientError as err:
            _LOGGER.warning("roomba_v4 AWS client error: method=%s url=%s error=%s", method, url, err)
            raise CloudApiError(f"AWS request transport error for {url}: {err}") from err

    async def _aws_request_detailed(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        retry: bool = True,
        accept: str = "application/json",
        method: str = "GET",
        payload: str = "",
    ) -> tuple[int, Any, str]:
        if not self.credentials:
            raise AuthenticationError("Not authenticated")
        region = self.credentials["CognitoId"].split(":")[0]
        signing_host, signing_path, final_url = self._resolve_execute_api_target(url)
        signer = AWSSignatureV4(
            access_key_id=self.credentials["AccessKeyId"],
            secret_access_key=self.credentials["SecretKey"],
            session_token=self.credentials["SessionToken"],
        )
        headers = signer.generate_signed_headers(
            method=method,
            service="execute-api",
            region=region,
            host=signing_host,
            path=signing_path,
            query_params=params or {},
            headers={"accept": accept, "content-type": "application/json", "user-agent": AWS_USER_AGENT},
            payload=payload,
        )
        final_url = final_url if not params else f"{final_url}?{urllib.parse.urlencode(params)}"
        request_coro = self.session.get if method.upper() == "GET" else self.session.post
        kwargs = {"headers": headers}
        if method.upper() != "GET":
            kwargs["data"] = payload.encode("utf-8")
        try:
            async with request_coro(final_url, **kwargs) as resp:
                txt = await resp.text()
                try:
                    data = json.loads(txt) if txt else {}
                except JSONDecodeError:
                    data = {"raw": txt}
                if resp.status == 403 and retry:
                    _LOGGER.debug("roomba_v4 AWS detailed request got 403, re-authenticating: method=%s url=%s", method, url)
                    await self.authenticate()
                    return await self._aws_request_detailed(url, params, retry=False, accept=accept, method=method, payload=payload)
                return resp.status, data, txt
        except asyncio.TimeoutError as err:
            _LOGGER.debug("roomba_v4 AWS detailed request timed out: method=%s url=%s", method, url)
            raise CloudApiError(f"AWS request timed out for {url}") from err
        except aiohttp.ClientError as err:
            _LOGGER.warning("roomba_v4 AWS detailed client error: method=%s url=%s error=%s", method, url, err)
            raise CloudApiError(f"AWS request transport error for {url}: {err}") from err

    async def get_livemap_mqtt_topic(self, robot_id: str) -> str | None:
        if not robot_id:
            return None
        url = f"{self.deployment['httpBaseAuth']}/v1/p2maps/livemap"
        params = {"robotId": robot_id}
        status, data, txt = await self._aws_request_detailed(url, params, method="GET")
        payload = {
            "robot_id": robot_id,
            "status": status,
            "request": {
                "url": url,
                "params": params,
            },
            "response": data,
            "response_text": txt[:2000],
        }
        await self._write_runtime_debug("livemap_topic_lookup", payload)
        if status != 200:
            raise CloudApiError(f"livemap topic lookup failed ({status}) {txt[:300]}")
        mqtt_topic = data.get("mqtt_topic") if isinstance(data, dict) else None
        if isinstance(mqtt_topic, str) and mqtt_topic:
            self._livemap_topics[robot_id] = mqtt_topic
            return mqtt_topic
        return None

    async def _aws_json_request(
        self,
        url: str,
        *,
        method: str = "POST",
        payload_obj: dict[str, Any] | list[Any] | None = None,
        params: dict[str, Any] | None = None,
        accept: str = "application/json",
    ) -> tuple[int, Any, str]:
        if not self.credentials:
            raise AuthenticationError("Not authenticated")
        payload = json.dumps(payload_obj or {}, separators=(",", ":"))
        region = self.credentials["CognitoId"].split(":")[0]
        parsed = urllib.parse.urlparse(url)
        signing_host, signing_path, final_url = self._resolve_execute_api_target(url)
        signer = AWSSignatureV4(
            access_key_id=self.credentials["AccessKeyId"],
            secret_access_key=self.credentials["SecretKey"],
            session_token=self.credentials["SessionToken"],
        )
        headers = signer.generate_signed_headers(
            method=method,
            service="execute-api",
            region=region,
            host=signing_host,
            path=signing_path,
            query_params=params or {},
            headers={"accept": accept, "content-type": "application/json", "user-agent": AWS_USER_AGENT},
            payload=payload,
        )
        final_url = final_url if not params else f"{final_url}?{urllib.parse.urlencode(params)}"
        async with self.session.request(method.upper(), final_url, headers=headers, data=payload.encode("utf-8")) as resp:
            txt = await resp.text()
            try:
                data = json.loads(txt) if txt else {}
            except JSONDecodeError:
                data = {"raw": txt}
            return resp.status, data, txt

    async def _aws_request_bytes(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        retry: bool = True,
        accept: str = "application/octet-stream",
    ) -> bytes:
        if not self.credentials:
            raise AuthenticationError("Not authenticated")
        region = self.credentials["CognitoId"].split(":")[0]
        parsed = urllib.parse.urlparse(url)
        signing_host, signing_path, final_url = self._resolve_execute_api_target(url)
        signer = AWSSignatureV4(
            access_key_id=self.credentials["AccessKeyId"],
            secret_access_key=self.credentials["SecretKey"],
            session_token=self.credentials["SessionToken"],
        )
        headers = signer.generate_signed_headers(
            method="GET",
            service="execute-api",
            region=region,
            host=signing_host,
            path=signing_path,
            query_params=params or {},
            headers={"accept": accept, "content-type": "application/json", "user-agent": AWS_USER_AGENT},
            payload="",
        )
        final_url = final_url if not params else f"{final_url}?{urllib.parse.urlencode(params)}"
        async with self.session.get(final_url, headers=headers) as resp:
            data = await resp.read()
            if resp.status == 403 and retry:
                await self.authenticate()
                return await self._aws_request_bytes(url, params, retry=False, accept=accept)
            if resp.status != 200:
                try:
                    txt = data.decode("utf-8", errors="ignore")
                except Exception:
                    txt = ""
                raise CloudApiError(f"AWS binary request failed ({resp.status}) {url}: {txt[:500]}")
            return data

    async def download_p2map_geojson(self, p2map_id: str, p2mapv_id: str) -> bytes:
        return await self._aws_request_bytes(
            f"{self.deployment['httpBaseAuth']}/v1/p2maps/{p2map_id}/versions/{p2mapv_id}/geojson",
            {"response_type": "binary"},
            accept="application/gzip,application/json",
        )

    async def get_pmaps(self, blid: str) -> list[dict[str, Any]]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/p2maps",
            {"visible": "true", "robotId": blid},
        )

    async def get_p2map_details(self, p2map_id: str) -> dict[str, Any]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/p2maps/{p2map_id}",
        )

    async def prime_livemap_session(self, robot_id: str) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "robot_id": robot_id,
            "visible_maps_count": 0,
            "selected_p2map_id": None,
            "selected_active_p2mapv_id": None,
            "steps": [],
        }
        if not robot_id:
            return summary

        visible_url = f"{self.deployment['httpBaseAuth']}/v1/p2maps"
        visible_params = {"visible": "true", "robotId": robot_id}
        status, data, txt = await self._aws_request_detailed(visible_url, visible_params, method="GET")
        visible_maps = data if isinstance(data, list) else []
        summary["visible_maps_count"] = len(visible_maps)
        summary["steps"].append({
            "step": "visible_maps",
            "status": status,
            "count": len(visible_maps),
            "response": data,
            "response_text": txt[:2000],
        })

        selected = None
        for item in visible_maps:
            if not isinstance(item, dict):
                continue
            if str(item.get("state") or "").lower() == "active":
                selected = item
                break
        if selected is None:
            for item in visible_maps:
                if isinstance(item, dict) and item.get("p2map_id"):
                    selected = item
                    break

        # Fallback to the last known p2map advertised in live state.
        if selected is None:
            live_p2maps = None
            shadow = self._live_state_sources.get("shadow") or {}
            if isinstance(shadow, dict):
                live_p2maps = shadow.get("p2maps")
            if isinstance(live_p2maps, list):
                for item in live_p2maps:
                    if isinstance(item, dict) and item.get("p2map_id"):
                        selected = {"p2map_id": item.get("p2map_id"), "active_p2mapv_id": item.get("p2mapv_id")}
                        break

        selected_p2map_id = selected.get("p2map_id") if isinstance(selected, dict) else None
        if isinstance(selected_p2map_id, str) and selected_p2map_id:
            summary["selected_p2map_id"] = selected_p2map_id
            detail_url = f"{self.deployment['httpBaseAuth']}/v1/p2maps/{selected_p2map_id}"
            detail_status, detail_data, detail_txt = await self._aws_request_detailed(detail_url, method="GET")
            if isinstance(detail_data, dict):
                summary["selected_active_p2mapv_id"] = (
                    detail_data.get("active_p2mapv_id")
                    or detail_data.get("p2mapv_id")
                    or (selected.get("active_p2mapv_id") if isinstance(selected, dict) else None)
                    or (selected.get("p2mapv_id") if isinstance(selected, dict) else None)
                )
            summary["steps"].append({
                "step": "selected_map_details",
                "status": detail_status,
                "p2map_id": selected_p2map_id,
                "response": detail_data,
                "response_text": detail_txt[:2000],
            })
        else:
            summary["steps"].append({
                "step": "selected_map_details",
                "status": None,
                "p2map_id": None,
                "note": "no_visible_or_shadow_p2map_found",
            })

        await self._write_runtime_debug("p2maps_prime", summary)
        return summary

    async def get_p2map_clean_score(self, p2map_id: str) -> dict[str, Any]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/p2maps/clean-score",
            {"p2map_id": p2map_id},
        )

    async def get_p2map_routines(self, p2map_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/p2maps/{p2map_id}/routines",
            {"limit": str(limit)},
        )

    async def get_mission_history(self, blid: str) -> dict[str, Any]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/{blid}/missionhistory",
            {"app_id": f"IOS-{self.config['appId']}", "filterType": "omit_quickly_canceled_not_scheduled", "supportedDoneCodes": "dndEnd,returnHomeEnd"},
        )

    async def get_pmap_umf(self, blid: str, pmap_id: str, version_id: str) -> dict[str, Any]:
        return await self._aws_request(
            f"{self.deployment['httpBaseAuth']}/v1/{blid}/pmaps/{pmap_id}/versions/{version_id}/umf",
            {"activeDetails": "2"},
        )

    async def get_favorites(self) -> dict[str, Any]:
        return await self._aws_request(f"{self.deployment['httpBaseAuth']}/v1/user/favorites")

    async def get_schedules(self) -> dict[str, Any]:
        return await self._aws_request(f"{self.deployment['httpBaseAuth']}/v1/user/automations")

    async def download_file(self, url: str) -> bytes:
        async with self.session.get(url) as resp:
            if resp.status != 200:
                txt = await resp.text()
                raise CloudApiError(f"Download failed ({resp.status}): {txt[:300]}")
            return await resp.read()


    def _redact_url_query_keys(self, url: str) -> dict[str, Any]:
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        return {"base": f"{parsed.scheme}://{parsed.netloc}{parsed.path}", "query_keys": sorted(query.keys())}

    async def _get_ssl_context(self) -> ssl.SSLContext:
        if self._ssl_context is None:
            self._ssl_context = await asyncio.to_thread(ssl.create_default_context)
        return self._ssl_context

    def _flatten_string_map(self, obj: Any, prefix: str = "") -> dict[str, str]:
        out: dict[str, str] = {}
        if isinstance(obj, dict):
            for key, value in obj.items():
                child_prefix = f"{prefix}.{key}" if prefix else str(key)
                if isinstance(value, str):
                    out[child_prefix] = value
                elif isinstance(value, dict):
                    out.update(self._flatten_string_map(value, child_prefix))
        return out

    def _all_token_sources(self) -> dict[str, str]:
        collected: dict[str, str] = {}
        for root_name, obj in (
            ("connection_tokens", (self.last_login_response or {}).get("connection_tokens") or {}),
            ("live_activity_tokens", (self.last_login_response or {}).get("live_activity_tokens") or {}),
            ("gigya_account_info", self.gigya_account_info or {}),
            ("deployment", self.deployment or {}),
        ):
            flattened = self._flatten_string_map(obj, root_name)
            for key, value in flattened.items():
                if value and len(value) < 4096:
                    collected[key] = value
        return collected

    def _exact_connection_token_bundle(self, robot_id: str) -> dict[str, str]:
        login = self.last_login_response or {}
        bundles = login.get("connection_tokens") or []
        if isinstance(bundles, dict):
            bundles = [bundles]
        if not isinstance(bundles, list):
            return {}
        for item in bundles:
            if not isinstance(item, dict):
                continue
            devices = item.get("devices") or []
            if isinstance(devices, list) and robot_id in devices:
                out = {}
                for key in ("client_id", "iot_token", "iot_signature", "iot_authorizer_name"):
                    value = item.get(key)
                    if isinstance(value, str) and value:
                        out[key] = value
                return out
        return {}

    def _exact_query_and_header_variants(self, robot_id: str) -> tuple[list[tuple[str, dict[str, str]]], list[tuple[str, Headers]], list[str]]:
        bundle = self._exact_connection_token_bundle(robot_id)
        query_variants: list[tuple[str, dict[str, str]]] = []
        header_variants: list[tuple[str, Headers]] = []
        client_ids: list[str] = []
        if not bundle:
            return query_variants, header_variants, client_ids

        authorizer = bundle.get("iot_authorizer_name")
        token = bundle.get("iot_token")
        signature = bundle.get("iot_signature")
        client_id = bundle.get("client_id")
        if client_id:
            client_ids.append(client_id)
        if authorizer and token and signature:
            # APK-confirmed websocket headers: x-amz-customauthorizer-name, x-amz-customauthorizer-signature, x-irobot-auth
            h = Headers()
            h["x-amz-customauthorizer-name"] = authorizer
            h["x-amz-customauthorizer-signature"] = signature
            h["x-irobot-auth"] = token
            header_variants.append(("apk_exact_headers", h))

            # Raw MQTT custom-auth can pass extra data in MQTT username query-string style.
            query_variants.append((
                "apk_exact_query",
                {
                    "x-amz-customauthorizer-name": authorizer,
                    "x-amz-customauthorizer-signature": signature,
                    "x-irobot-auth": token,
                },
            ))
        return query_variants, header_variants, client_ids

    def _authorizer_candidates(self) -> list[str]:
        flattened = self._all_token_sources()
        names = ["default-authorizer"]
        for key, value in flattened.items():
            kl = key.lower()
            if any(tag in kl for tag in ["authorizer", "authorizername", "auth_name", "customauthorizer"]):
                if value not in names:
                    names.append(value)
        return names[:6]

    def _extract_token_like_values(self) -> list[tuple[str, str]]:
        flattened = self._all_token_sources()
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        priority_tags = ["jwt", "token", "signature", "value", "password", "secret", "key"]
        for key, value in flattened.items():
            kl = key.lower()
            if any(tag in kl for tag in priority_tags):
                if value not in seen and len(value) >= 8:
                    seen.add(value)
                    out.append((key, value))
        return out[:12]

    def _connection_token_query_variants(self) -> list[tuple[str, dict[str, str]]]:
        flattened = self._all_token_sources()
        variants: list[tuple[str, dict[str, str]]] = []
        if not flattened:
            return variants

        token_like = self._extract_token_like_values()
        authorizer_names = self._authorizer_candidates()

        # Bare authorizer-only variants.
        for authorizer in authorizer_names:
            variants.append(("authorizer_only", {"X-Amz-CustomAuthorizer-Name": authorizer}))
            variants.append(("authorizer_only_lower", {"x-amz-customauthorizer-name": authorizer}))

        # Common AWS IoT custom-authorizer query conventions.
        token_param_names = [
            "token",
            "auth",
            "jwt",
            "x-amz-customauthorizer-signature",
            "X-Amz-CustomAuthorizer-Signature",
            "x-amz-customauthorizer-token",
            "X-Amz-CustomAuthorizer-Token",
            "password",
            "username",
        ]
        for _, token in token_like[:6]:
            for param_name in token_param_names:
                variants.append((f"{param_name}_only", {param_name: token}))
                for authorizer in authorizer_names:
                    variants.append((f"{param_name}_with_authorizer", {
                        "x-amz-customauthorizer-name": authorizer,
                        param_name: token,
                    }))

        # Include a raw flattened pass-through for diagnostic value, capped.
        raw_variant: dict[str, str] = {}
        for key, value in list(flattened.items())[:8]:
            leaf = key.split(".")[-1]
            if len(value) < 512:
                raw_variant[leaf] = value
        if raw_variant:
            variants.append(("raw_connection_tokens", raw_variant))

        dedup: list[tuple[str, dict[str, str]]] = []
        seen: set[tuple[tuple[str, str], ...]] = set()
        for name, params in variants:
            key = tuple(sorted(params.items()))
            if key and key not in seen:
                seen.add(key)
                dedup.append((name, params))
        return dedup

    def _aws_iot_presigned_wss_url(
        self,
        endpoint: str,
        *,
        region: str,
        expires: int = 900,
        extra_query: dict[str, str] | None = None,
        host_header: str | None = None,
        include_port_in_url: bool = False,
        use_unsigned_payload: bool = False,
        path: str = "/mqtt",
    ) -> str:
        if not self.credentials:
            raise AuthenticationError("Not authenticated")
        signer = AWSSignatureV4(
            access_key_id=self.credentials["AccessKeyId"],
            secret_access_key=self.credentials["SecretKey"],
            session_token=self.credentials["SessionToken"],
        )
        now = datetime.now(tz=UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        service = "iotdevicegateway"
        canonical_uri = path
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        query: dict[str, str] = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{self.credentials['AccessKeyId']}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires),
            "X-Amz-SignedHeaders": "host",
        }
        if self.credentials.get("SessionToken"):
            query["X-Amz-Security-Token"] = self.credentials["SessionToken"]
        if extra_query:
            query.update(extra_query)
        signing_host = host_header or endpoint
        canonical_query = "&".join(
            f"{urllib.parse.quote(str(k), safe='~')}={urllib.parse.quote(str(v), safe='~')}"
            for k, v in sorted(query.items())
        )
        canonical_headers = f"host:{signing_host}\n"
        signed_headers = "host"
        payload_hash = "UNSIGNED-PAYLOAD" if use_unsigned_payload else hashlib.sha256(b"").hexdigest()
        canonical_request = (
            f"GET\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )
        signing_key = signer._get_signature_key(date_stamp, region, service)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        query["X-Amz-Signature"] = signature
        netloc = endpoint + (":443" if include_port_in_url and ":" not in endpoint else "")
        return f"wss://{netloc}{canonical_uri}?{urllib.parse.urlencode(query)}"

    @staticmethod
    def _mqtt_encode_remaining_length(length: int) -> bytes:
        encoded = bytearray()
        while True:
            digit = length % 128
            length //= 128
            if length > 0:
                digit |= 0x80
            encoded.append(digit)
            if length == 0:
                break
        return bytes(encoded)

    @staticmethod
    def _mqtt_encode_utf8(s: str) -> bytes:
        b = s.encode("utf-8")
        return len(b).to_bytes(2, "big") + b

    def _mqtt_connect_packet(self, client_id: str, username: str | None = None, password: str | None = None, protocol_level: int = 4, keepalive: int = 60) -> bytes:
        connect_flags = 0x02
        payload = self._mqtt_encode_utf8(client_id)
        if username is not None:
            connect_flags |= 0x80
            payload += self._mqtt_encode_utf8(username)
        if password is not None:
            connect_flags |= 0x40
            payload += self._mqtt_encode_utf8(password)
        proto_name = "MQTT" if protocol_level == 4 else "MQIsdp"
        variable = self._mqtt_encode_utf8(proto_name) + bytes([protocol_level, connect_flags]) + int(keepalive).to_bytes(2, "big")
        return bytes([0x10]) + self._mqtt_encode_remaining_length(len(variable)+len(payload)) + variable + payload

    @staticmethod
    def _hex_bytes(data: bytes) -> str:
        return data.hex() if data else "<empty>"

    async def _write_packet_hex_trace(self, name: str, payload: dict[str, Any]) -> None:
        try:
            await self._write_runtime_debug(name, payload)
        except Exception:
            _LOGGER.debug("v118 packet hex trace write failed for %s", name, exc_info=True)

    def _build_custom_authorizer_usernames(self, robot_id: str) -> list[tuple[str, str, str | None, list[str]]]:
        bundle = self._exact_connection_token_bundle(robot_id)
        if not bundle:
            return []
        authorizer = bundle.get("iot_authorizer_name")
        token = bundle.get("iot_token")
        signature = bundle.get("iot_signature")
        if not authorizer or not token:
            return []
        items: list[tuple[str, str, str | None, list[str]]] = []
        username = (
            f"mqtt?x-amz-customauthorizer-name={urllib.parse.quote(authorizer, safe='')}"
            f"&x-amz-customauthorizer-signature={urllib.parse.quote(signature or '', safe='')}"
            f"&x-irobot-auth={urllib.parse.quote(token, safe='')}"
        )
        items.append(("apk_style_username_query", username, None, ["iot_authorizer_name", "iot_signature", "iot_token"]))
        return items

    async def _publish_via_tls_mqtt_custom_authorizer(self, *, endpoint: str, robot_id: str, topics: list[str], payload_variants: list[tuple[str, dict[str, Any]]], client_ids: list[str]) -> dict[str, Any] | None:
        usernames = self._build_custom_authorizer_usernames(robot_id)
        if not usernames:
            return None
        ssl_context = await self._get_ssl_context()
        try:
            ssl_context.set_alpn_protocols(["mqtt"])
        except Exception:
            pass

        bundle = self._exact_connection_token_bundle(robot_id)
        exact_client_id = bundle.get("client_id") if bundle else None
        filtered_client_ids: list[str] = []
        if isinstance(exact_client_id, str) and exact_client_id:
            filtered_client_ids.append(exact_client_id)
        for cid in client_ids:
            if cid.startswith("IOS-") and cid not in filtered_client_ids:
                filtered_client_ids.append(cid)
        if not filtered_client_ids:
            filtered_client_ids = client_ids[:2]

        attempts=[]
        for uname_name, username, password, key_names in usernames:
            for protocol_level in (4, 3):
                for client_id in filtered_client_ids:
                    info={"transport":"tls_mqtt_custom_authorizer","client_id":client_id,"username_variant":uname_name,"username_preview":username[:120],"password":("<set>" if password else None),"token_keys":key_names,"protocol_level":protocol_level}
                    _LOGGER.debug("v60 TLS MQTT attempt client_id=%s username_variant=%s proto=%s", client_id, uname_name, protocol_level)
                    try:
                        reader, writer = await asyncio.wait_for(asyncio.open_connection(endpoint, 443, ssl=ssl_context, server_hostname=endpoint), timeout=8.0)
                        try:
                            _LOGGER.debug("v60 TLS MQTT connected client_id=%s username_variant=%s proto=%s", client_id, uname_name, protocol_level)
                            connect_packet = self._mqtt_connect_packet(client_id, username=username, password=password, protocol_level=protocol_level)
                            _LOGGER.debug("v60 CONNECT HEX client_id=%s username_variant=%s proto=%s hex=%s", client_id, uname_name, protocol_level, self._hex_bytes(connect_packet))
                            writer.write(connect_packet)
                            await writer.drain()
                            _LOGGER.debug("v60 TLS MQTT CONNECT sent client_id=%s username_variant=%s proto=%s", client_id, uname_name, protocol_level)
                            first = await asyncio.wait_for(reader.read(8), timeout=2.0)
                            extra = await asyncio.wait_for(reader.read(32), timeout=1.0)
                            info["first_hex"] = self._hex_bytes(first)
                            info["extra_hex"] = self._hex_bytes(extra)
                            _LOGGER.debug("v60 MQTT first_bytes client_id=%s username_variant=%s proto=%s len=%s hex=%s", client_id, uname_name, protocol_level, len(first), self._hex_bytes(first))
                            _LOGGER.debug("v60 MQTT extra_bytes client_id=%s username_variant=%s proto=%s len=%s hex=%s", client_id, uname_name, protocol_level, len(extra), self._hex_bytes(extra))
                            if len(first) >= 4 and first[0] == 0x20:
                                rc = first[3]
                                info["connack_rc"] = rc
                                if rc == 0:
                                    _LOGGER.debug("v60 MQTT CONNACK success client_id=%s username_variant=%s proto=%s", client_id, uname_name, protocol_level)
                                    # publish first payload/topic only
                                    topic = topics[0]
                                    variant_name, body = payload_variants[0]
                                    payload_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
                                    writer.write(self._mqtt_publish_packet(topic, payload_bytes))
                                    await writer.drain()
                                    return {"status":"published","transport":"tls_mqtt_custom_authorizer","endpoint":endpoint,"client_id":client_id,"username_variant":uname_name,"protocol_level":protocol_level,"topic":topic,"payload_variant":variant_name,"first_hex":self._hex_bytes(first),"extra_hex":self._hex_bytes(extra)}
                                info["error"] = f"connack_rc_{rc}"
                            elif not first:
                                info["error"] = "empty_first_bytes"
                            else:
                                info["error"] = "non_connack_response"
                            _LOGGER.debug("v60 TLS MQTT rejected client_id=%s username_variant=%s proto=%s reason=%s", client_id, uname_name, protocol_level, info.get("error"))
                            attempts.append(info)
                        finally:
                            try:
                                writer.close(); await writer.wait_closed()
                            except Exception:
                                pass
                    except Exception as err:
                        info["error"]=str(err)
                        attempts.append(info)
                        _LOGGER.debug("v60 TLS MQTT exception client_id=%s username_variant=%s proto=%s error=%r", client_id, uname_name, protocol_level, err)
        await self._write_runtime_debug("tls_mqtt_custom_authorizer_failed", {"attempts": attempts[:40]})
        return None

    def _mqtt_publish_packet(self, topic: str, payload: bytes) -> bytes:
        variable = self._mqtt_encode_utf8(topic)
        remaining = len(variable) + len(payload)
        return bytes([0x30]) + self._mqtt_encode_remaining_length(remaining) + variable + payload

    def _mqtt_subscribe_packet(self, packet_id: int, topics: list[str], qos: int = 1) -> bytes:
        qos_byte = bytes([max(0, min(2, int(qos)))])
        payload = b"".join(self._mqtt_encode_utf8(topic) + qos_byte for topic in topics)
        variable = packet_id.to_bytes(2, "big")
        remaining = len(variable) + len(payload)
        return bytes([0x82]) + self._mqtt_encode_remaining_length(remaining) + variable + payload

    def _mqtt_decode_publish(self, data: bytes) -> dict[str, Any]:
        if len(data) < 4:
            return {"type": 3, "raw_hex": data.hex(), "raw_len": len(data), "topic": None, "payload_text": None}
        qos = (data[0] >> 1) & 0x03
        idx = 1
        multiplier = 1
        remaining = 0
        while idx < len(data):
            digit = data[idx]
            remaining += (digit & 0x7F) * multiplier
            multiplier *= 128
            idx += 1
            if not (digit & 0x80):
                break
        start = idx
        if start + 2 > len(data):
            return {"type": 3, "raw_hex": data.hex(), "raw_len": len(data), "topic": None, "payload_text": None}
        topic_len = int.from_bytes(data[start:start+2], "big")
        start += 2
        topic = data[start:start+topic_len].decode("utf-8", errors="replace")
        start += topic_len
        if qos:
            start += 2
        payload = data[start:]
        try:
            payload_text = payload.decode("utf-8")
        except Exception:
            payload_text = None
        decoded = {"type": 3, "raw_hex": data.hex(), "raw_len": len(data), "topic": topic, "payload_text": payload_text}
        if payload_text:
            try:
                decoded["payload_json"] = json.loads(payload_text)
            except Exception:
                pass
        return decoded

    async def _mqtt_read_packet(self, websocket) -> dict[str, Any]:
        frame = await websocket.recv()
        if isinstance(frame, str):
            data = frame.encode("utf-8")
        else:
            data = bytes(frame)
        if not data:
            return {"type": "empty", "raw_hex": ""}
        packet_type = data[0] >> 4
        if packet_type == 3:
            return self._mqtt_decode_publish(data)
        result = {
            "type": packet_type,
            "raw_hex": data.hex(),
            "raw_len": len(data),
            "session_present": bool(len(data) > 2 and data[2] & 0x01) if packet_type == 2 else None,
            "return_code": data[3] if packet_type == 2 and len(data) > 3 else None,
        }
        if packet_type == 9 and len(data) >= 5:
            result["packet_id"] = int.from_bytes(data[2:4], "big")
            result["suback_codes"] = list(data[4:])
        return result

    async def _mqtt_read_stream_packet(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        first = await reader.readexactly(1)
        if not first:
            return {"type": "empty", "raw_hex": ""}
        remaining_bytes = bytearray()
        multiplier = 1
        remaining = 0
        while True:
            digit = await reader.readexactly(1)
            remaining_bytes.extend(digit)
            remaining += (digit[0] & 0x7F) * multiplier
            if not (digit[0] & 0x80):
                break
            multiplier *= 128
        payload = await reader.readexactly(remaining) if remaining else b""
        data = bytes(first) + bytes(remaining_bytes) + payload
        packet_type = data[0] >> 4
        if packet_type == 3:
            return self._mqtt_decode_publish(data)
        result = {
            "type": packet_type,
            "raw_hex": data.hex(),
            "raw_len": len(data),
            "session_present": bool(len(data) > 3 and data[2] & 0x01) if packet_type == 2 else None,
            "return_code": data[3] if packet_type == 2 and len(data) > 3 else None,
        }
        if packet_type == 9 and len(data) >= 5:
            result["packet_id"] = int.from_bytes(data[2:4], "big")
            result["suback_codes"] = list(data[4:])
        return result

    async def _get_local_ssl_context(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            ctx.set_alpn_protocols(["mqtt"])
        except Exception:
            pass
        return ctx

    def _local_mqtt_host_candidates(self, robot_id: str) -> list[str]:
        robot = self.robots.get(robot_id) or {}
        out: list[str] = []
        for value in [
            self.local_mqtt_ip_override,
            robot.get("ip"),
            robot.get("ipAddress"),
            robot.get("ipv4"),
            robot.get("localIp"),
            robot.get("hostname"),
        ]:
            if isinstance(value, str) and value and value not in out:
                out.append(value)
        return out

    async def _try_local_mqtt_subscriber(self, robot_id: str, cycle: int) -> bool:
        robot = self.robots.get(robot_id) or {}
        mqtt_password = robot.get("password")
        hosts = self._local_mqtt_host_candidates(robot_id)
        attempts: list[dict[str, Any]] = []
        if not hosts or not isinstance(mqtt_password, str) or not mqtt_password:
            await self._write_runtime_debug("local_subscriber_attempts", {
                "cycle": cycle,
                "connected": False,
                "hosts": hosts,
                "reason": "missing host or password",
            })
            return False

        ssl_context = await self._get_local_ssl_context()
        topics = ["#"]
        client_ids = [f"HA-local-{self.config['deviceId'][:8]}", f"IOS-{self.config['appId']}", robot_id[:10]]
        username_variants = ["user", robot_id, None]
        for host in hosts:
            for username in username_variants:
                for client_id in client_ids:
                    attempt = {
                        "cycle": cycle,
                        "transport": "local_tls_mqtt",
                        "host": host,
                        "port": self.local_mqtt_port,
                        "client_id": client_id,
                        "username": username,
                        "topics": topics,
                    }
                    attempts.append(attempt)
                    try:
                        await self._log_event_status({
                            "status": "starting",
                            "transport": "local_tls_mqtt",
                            "host": host,
                            "port": self.local_mqtt_port,
                            "client_id": client_id,
                            "username": username,
                            "cycle": cycle,
                        })
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(host, self.local_mqtt_port, ssl=ssl_context, server_hostname=None),
                            timeout=8.0,
                        )
                        attempt["socket_opened"] = True
                        writer.write(self._mqtt_connect_packet(client_id, username=username, password=mqtt_password))
                        await writer.drain()
                        connack = await asyncio.wait_for(self._mqtt_read_stream_packet(reader), timeout=5)
                        attempt["connack"] = connack
                        await self._append_event_packet({
                            "ts": datetime.now(tz=UTC).isoformat(),
                            "type": "local_connack",
                            "topic": None,
                            "payload_json": connack,
                            "payload_text": None,
                        })
                        if connack.get("type") != 2 or connack.get("return_code") not in {0, None}:
                            raise CloudApiError(f"bad local connack: {connack}")

                        writer.write(self._mqtt_subscribe_packet(1, topics, qos=1))
                        await writer.drain()
                        suback = await asyncio.wait_for(self._mqtt_read_stream_packet(reader), timeout=5)
                        attempt["suback"] = suback
                        await self._append_event_packet({
                            "ts": datetime.now(tz=UTC).isoformat(),
                            "type": "local_suback",
                            "topic": None,
                            "payload_json": suback,
                            "payload_text": None,
                        })
                        if suback.get("type") != 9:
                            raise CloudApiError(f"bad local suback: {suback}")

                        await self._log_event_status({
                            "status": "subscribed",
                            "transport": "local_tls_mqtt",
                            "host": host,
                            "port": self.local_mqtt_port,
                            "client_id": client_id,
                            "username": username,
                            "cycle": cycle,
                        })
                        await self._write_runtime_debug("local_subscriber_connected", attempt)

                        saw_message = False
                        last_packet_ts = asyncio.get_running_loop().time()
                        while not self._subscriber_stop.is_set():
                            try:
                                packet = await asyncio.wait_for(self._mqtt_read_stream_packet(reader), timeout=20)
                            except asyncio.TimeoutError:
                                writer.write(b"\xc0\x00")
                                await writer.drain()
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "local_pingreq",
                                    "topic": None,
                                    "payload_json": {"idle_seconds": round(asyncio.get_running_loop().time() - last_packet_ts, 3)},
                                    "payload_text": None,
                                })
                                continue
                            if packet.get("type") == 3:
                                saw_message = True
                                last_packet_ts = asyncio.get_running_loop().time()
                                packet["transport"] = "local_tls_mqtt"
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "message",
                                    "topic": packet.get("topic"),
                                    "payload_json": packet.get("payload_json"),
                                    "payload_text": packet.get("payload_text"),
                                })
                            elif packet.get("type") == 13:
                                last_packet_ts = asyncio.get_running_loop().time()
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "local_pingresp",
                                    "topic": None,
                                    "payload_json": packet,
                                    "payload_text": None,
                                })
                            else:
                                last_packet_ts = asyncio.get_running_loop().time()
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "local_packet",
                                    "topic": packet.get("topic"),
                                    "payload_json": packet,
                                    "payload_text": packet.get("payload_text"),
                                })
                            if saw_message:
                                attempt["saw_message"] = True

                        try:
                            writer.close()
                            await writer.wait_closed()
                        except Exception:
                            pass
                        return True
                    except Exception as err:
                        attempt["error"] = str(err)
                        await self._log_event_status({
                            "status": "disconnected",
                            "transport": "local_tls_mqtt",
                            "host": host,
                            "port": self.local_mqtt_port,
                            "client_id": client_id,
                            "username": username,
                            "cycle": cycle,
                            "error": str(err),
                        })
                        try:
                            writer.close()
                            await writer.wait_closed()
                        except Exception:
                            pass
                        continue
        await self._write_runtime_debug("local_subscriber_attempts", {"cycle": cycle, "connected": False, "attempts": attempts[:80]})
        return False

    def _preferred_payload_order(self, commanddef: dict[str, Any]) -> list[str]:
        preferred = commanddef.get("_preferred_payload_variant")
        base = ["raw_commanddef_payload", "robot_command_envelope", "commanddef_wrapped", "apk_commanddefs_upper", "apk_commanddefs_lower", "mission_shadow_desired"]
        if isinstance(preferred, str) and preferred in base:
            return [preferred] + [name for name in base if name != preferred]
        return base

    def _topic_merge_mode(self, topic: str | None) -> str:
        topic_l = str(topic or "").lower()
        if "/ro-currentstate/" in topic_l:
            return "currentstate"
        if "/ro-stats/" in topic_l:
            return "stats"
        if "/rw-software/" in topic_l:
            return "software"
        if "/livemap/" in topic_l:
            return "livemap"
        if "/shadow/get/accepted" in topic_l or "/shadow/update/" in topic_l:
            return "shadow"
        return "other"

    def _topic_source_kind(self, topic: str | None) -> str:
        topic_l = str(topic or "").lower()
        if "/ro-currentstate/update/accepted" in topic_l:
            return "currentstate_update"
        if "/ro-currentstate/get/accepted" in topic_l:
            return "currentstate_get"
        if "/ro-stats/" in topic_l:
            return "stats"
        if "/livemap/" in topic_l:
            return "livemap"
        if "/shadow/get/accepted" in topic_l or "/shadow/update/" in topic_l:
            return "shadow"
        return "other"

    def _filter_fragment_for_topic(self, fragment: dict[str, Any], mode: str) -> dict[str, Any]:
        if not isinstance(fragment, dict):
            return {}
        if mode == "currentstate":
            return {
                k: v
                for k, v in fragment.items()
                if k in {"cleanMissionStatus", "pose", "dock", "batPct", "bin", "tankPresent", "detectedPad", "signal", "bbrun", "bbmssn", "name", "sku"}
            }
        if mode == "stats":
            return {k: v for k, v in fragment.items() if k in {"batPct", "signal", "bin"}}
        if mode == "livemap":
            return {
                k: v
                for k, v in fragment.items()
                if k in {"cleanMissionStatus", "pose", "dock", "batPct", "signal", "bin"}
            }
        if mode == "shadow":
            if isinstance(self._live_state.get("cleanMissionStatus"), dict):
                return {}
            return {
                k: v
                for k, v in fragment.items()
                if k in {"cleanMissionStatus", "pose", "dock", "batPct", "bin", "tankPresent", "detectedPad", "signal"}
            }
        return {}

    def _extract_livemap_fragment(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}

        candidates: list[dict[str, Any]] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                candidates.append(node)
                for value in node.values():
                    visit(value)
            elif isinstance(node, list):
                for item in node:
                    visit(item)

        visit(payload)

        pose_fragment: dict[str, Any] = {}
        mission_fragment: dict[str, Any] = {}
        extras: dict[str, Any] = {}

        def first_number(*values: Any) -> Any:
            for value in values:
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    return value
            return None

        def parse_cur_path(values: Any) -> dict[str, Any]:
            if not isinstance(values, list) or len(values) < 6:
                return {}

            work = list(values)
            path_kind = work[0] if work else None
            body = work[1:]
            trailing_timestamp = None
            if body and isinstance(body[-1], (int, float)) and not isinstance(body[-1], bool) and body[-1] > 1_000_000_000:
                trailing_timestamp = int(body[-1])
                body = body[:-1]

            numeric_body = [
                value for value in body
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            ]
            usable_len = (len(numeric_body) // 4) * 4
            numeric_body = numeric_body[:usable_len]
            if usable_len < 4:
                return {}

            groups: list[dict[str, Any]] = []
            for idx in range(0, usable_len, 4):
                x, y, theta, flag = numeric_body[idx:idx + 4]
                groups.append({
                    "x": x,
                    "y": y,
                    "theta": theta,
                    "flag": flag,
                })

            if not groups:
                return {}

            last = groups[-1]
            current_pose = {
                "point": {
                    "x": last["x"],
                    "y": last["y"],
                },
                "theta": last["theta"],
            }
            return {
                "pose": current_pose,
                "livemap": {
                    "path_kind": path_kind,
                    "path_header": path_kind,
                    "path_points": groups[-50:],
                    "path_points_count": len(groups),
                    "current": {
                        "x": last["x"],
                        "y": last["y"],
                        "theta": last["theta"],
                        "flag": last.get("flag"),
                    },
                    "raw_count": len(values),
                    "usable_count": usable_len,
                    **({"path_timestamp": trailing_timestamp} if trailing_timestamp is not None else {}),
                    **({"update_expire_ts": payload.get("update_expire_ts")} if isinstance(payload.get("update_expire_ts"), (int, float)) else {}),
                    **({"timestamp": payload.get("timestamp")} if isinstance(payload.get("timestamp"), (int, float)) else {}),
                },
            }

        pos_update = payload.get("pos_update") if isinstance(payload.get("pos_update"), dict) else None
        if pos_update and isinstance(pos_update.get("cur_path"), list):
            parsed = parse_cur_path(pos_update.get("cur_path"))
            if parsed:
                pose_fragment = parsed

        if not pose_fragment:
            for item in candidates:
                pose = item.get("pose") if isinstance(item.get("pose"), dict) else None
                point = pose.get("point") if isinstance(pose, dict) and isinstance(pose.get("point"), dict) else None
                x = first_number(
                    (point or {}).get("x"),
                    item.get("x"),
                    item.get("posX"),
                    item.get("px"),
                )
                y = first_number(
                    (point or {}).get("y"),
                    item.get("y"),
                    item.get("posY"),
                    item.get("py"),
                )
                theta = first_number(
                    (pose or {}).get("theta"),
                    item.get("theta"),
                    item.get("heading"),
                    item.get("angle"),
                )
                if x is not None or y is not None or theta is not None:
                    pose_fragment = {
                        "pose": {
                            "point": {
                                **({"x": x} if x is not None else {}),
                                **({"y": y} if y is not None else {}),
                            },
                            **({"theta": theta} if theta is not None else {}),
                        }
                    }
                    break

        cms_keys = {"phase", "cycle", "error", "notReady", "mssnM", "sqft"}
        for item in candidates:
            cms = item.get("cleanMissionStatus") if isinstance(item.get("cleanMissionStatus"), dict) else None
            if cms:
                mission_fragment = {"cleanMissionStatus": {k: cms.get(k) for k in cms_keys if k in cms}}
                break
            if any(key in item for key in cms_keys):
                mission_fragment = {"cleanMissionStatus": {k: item.get(k) for k in cms_keys if k in item}}
                break

        for item in candidates:
            if "batPct" in item and item.get("batPct") is not None:
                extras["batPct"] = item.get("batPct")
                break
        for item in candidates:
            if isinstance(item.get("dock"), dict):
                extras["dock"] = item.get("dock")
                break
        for item in candidates:
            if isinstance(item.get("signal"), dict):
                extras["signal"] = item.get("signal")
                break
        fragment: dict[str, Any] = {}
        for part in (pose_fragment, mission_fragment, extras):
            if isinstance(part, dict) and part:
                self._deep_merge_dict(fragment, part)
        return fragment

    def _build_mqtt_payload_variants(self, commanddef: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        robot_id = str(commanddef.get("robot_id") or "")
        command = str(commanddef.get("command") or "start")
        command_lower = command.lower()
        command_upper = command.upper()
        params = commanddef.get("params") or {}
        params_dict = dict(params) if isinstance(params, dict) else {}
        p2map_id = commanddef.get("p2map_id")
        pmapv_id = commanddef.get("user_p2mapv_id") or commanddef.get("pmapv_id")

        # APK analysis showed the app serializes a CommandListDef under `commanddefs`.
        # Each item is a RoutineCommand using fields like command, robot_id, ordered,
        # params, p2map_id, user_p2mapv_id and select_all.
        params_upper = dict(params_dict)
        params_lower = dict(params_dict)
        if command_lower == "start":
            existing_rt = str(params_dict.get("routine_type") or "")
            if existing_rt:
                params_upper.setdefault("routine_type", existing_rt.upper())
                params_lower.setdefault("routine_type", existing_rt.lower())
            else:
                params_upper.setdefault("routine_type", "CLEAN_ALL")
                params_lower.setdefault("routine_type", "clean_all")

        routine_upper: dict[str, Any] = {
            "command": command_upper,
            "robot_id": robot_id,
            "ordered": 0,
            "params": params_upper,
        }
        routine_lower: dict[str, Any] = {
            "command": command_lower,
            "robot_id": robot_id,
            "ordered": 0,
            "params": params_lower,
        }
        if p2map_id:
            routine_upper["p2map_id"] = p2map_id
            routine_lower["p2map_id"] = p2map_id
        if pmapv_id:
            routine_upper["user_p2mapv_id"] = pmapv_id
            routine_lower["user_p2mapv_id"] = pmapv_id
        if "select_all" in commanddef:
            routine_upper["select_all"] = bool(commanddef.get("select_all"))
            routine_lower["select_all"] = bool(commanddef.get("select_all"))
        elif command_lower == "start":
            routine_upper["select_all"] = True
            routine_lower["select_all"] = True

        regions = commanddef.get("regions")
        if isinstance(regions, list) and regions:
            routine_upper["regions"] = regions
            routine_lower["regions"] = regions

        payload_obj: dict[str, Any] = {
            "command": command_lower,
            "time": int(datetime.now(tz=UTC).timestamp()),
            "initiator": "localApp",
        }
        if isinstance(params, dict):
            payload_obj.update(params)
        if p2map_id:
            payload_obj["p2map_id"] = p2map_id
        if isinstance(regions, list) and regions:
            payload_obj["regions"] = regions
        if "select_all" in commanddef:
            payload_obj["select_all"] = bool(commanddef.get("select_all"))

        variants: list[tuple[str, dict[str, Any]]] = [
            ("apk_commanddefs_upper", {"commanddefs": [routine_upper]}),
            ("apk_commanddefs_lower", {"commanddefs": [routine_lower]}),
            ("raw_commanddef_payload", payload_obj),
            (
                "robot_command_envelope",
                {
                    "robot_id": robot_id,
                    "command": payload_obj.get("command"),
                    "params": params if isinstance(params, dict) else {},
                    "p2map_id": p2map_id,
                    "initiator": "localApp",
                    "time": payload_obj["time"],
                    **({"ordered": int(commanddef.get("ordered"))} if commanddef.get("ordered") is not None else {}),
                    **({"user_p2mapv_id": pmapv_id} if pmapv_id else {}),
                    **({"regions": regions} if isinstance(regions, list) and regions else {}),
                    **({"select_all": bool(commanddef.get("select_all"))} if "select_all" in commanddef else {}),
                },
            ),
            (
                "commanddef_wrapped",
                {
                    "robot_id": robot_id,
                    "command": commanddef.get("command") or payload_obj.get("command"),
                    "commanddef": commanddef,
                    "initiator": "localApp",
                    "time": payload_obj["time"],
                },
            ),
            (
                "mission_shadow_desired",
                {
                    "state": {
                        "desired": {
                            "robot_id": robot_id,
                            "command": payload_obj.get("command"),
                            "initiator": "localApp",
                            "time": payload_obj["time"],
                            **({"params": params} if isinstance(params, dict) and params else {}),
                            **({"p2map_id": p2map_id} if p2map_id else {}),
                            **({"regions": regions} if isinstance(regions, list) and regions else {}),
                            **({"select_all": bool(commanddef.get("select_all"))} if "select_all" in commanddef else {}),
                        }
                    }
                },
            ),
        ]
        return variants

    def _is_room_clean_commanddef(self, commanddef: dict[str, Any]) -> bool:
        regions = commanddef.get("regions")
        return bool(commanddef.get("command") == "start" and commanddef.get("select_all") is False and isinstance(regions, list) and len(regions) > 0)

    def _room_clean_single_publish_plan(self, commanddef: dict[str, Any], irbt_topics: str, robot_id: str) -> tuple[list[str], list[tuple[str, dict[str, Any]]]]:
        payload_variants = self._build_mqtt_payload_variants(commanddef)
        by_name = {name: body for name, body in payload_variants}
        preferred_order = self._preferred_payload_order(commanddef)
        forced_variant = commanddef.get("_room_single_variant")
        schema_name = commanddef.get("_room_region_schema")
        if forced_variant and forced_variant in by_name:
            selected = (forced_variant, by_name[forced_variant])
        else:
            selected = next(((name, by_name[name]) for name in preferred_order if name in by_name), payload_variants[:1][0])
        topics = [f"{irbt_topics}/things/{robot_id}/cmd"]
        return topics, [selected]

    def _custom_authorizer_header_variants(self) -> list[tuple[str, Headers]]:
        # Keep the heuristic pool small. The APK showed a very specific websocket-header trio.
        variants: list[tuple[str, Headers]] = [("none", Headers())]
        for authorizer in self._authorizer_candidates():
            h = Headers()
            h["x-amz-customauthorizer-name"] = authorizer
            variants.append((f"authorizer_header_{authorizer}", h))
        return variants

    def add_live_state_listener(self, callback) -> None:
        if callback not in self._live_state_listeners:
            self._live_state_listeners.append(callback)

    def remove_live_state_listener(self, callback) -> None:
        try:
            self._live_state_listeners.remove(callback)
        except ValueError:
            pass

    def _compose_effective_live_state(self) -> dict[str, Any]:
        state: dict[str, Any] = {}
        merge_order = ["shadow", "currentstate_get", "currentstate_update", "livemap"]
        for name in merge_order:
            fragment = self._live_state_sources.get(name) or {}
            if isinstance(fragment, dict) and fragment:
                self._deep_merge_dict(state, fragment)
        stats = self._live_state_sources.get("stats") or {}
        if isinstance(stats, dict):
            allowed_stats = {k: v for k, v in stats.items() if k in {"batPct", "signal", "bin"}}
            self._deep_merge_dict(state, allowed_stats)
        meta = state.get("_meta") if isinstance(state.get("_meta"), dict) else {}
        if not isinstance(meta, dict):
            meta = {}
        meta["source_priority"] = [
            name for name in merge_order
            if isinstance(self._live_state_sources.get(name), dict) and self._live_state_sources.get(name)
        ]
        state["_meta"] = meta
        return state

    def get_live_state_snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self._live_state)

    def _deep_merge_dict(self, base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        for key, value in incoming.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                self._deep_merge_dict(base[key], value)
            else:
                base[key] = copy.deepcopy(value)
        return base

    def _extract_shadow_state_fragments(self, payload: Any) -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []

        def add(label: str, value: Any) -> None:
            if isinstance(value, dict) and value:
                out.append((label, value))

        if not isinstance(payload, dict):
            return out

        state = payload.get("state")
        if isinstance(state, dict):
            add("state", state)
            add("state.reported", state.get("reported"))
            add("state.desired", state.get("desired"))

        current = payload.get("current")
        if isinstance(current, dict):
            add("current", current)
            current_state = current.get("state")
            if isinstance(current_state, dict):
                add("current.state", current_state)
                add("current.state.reported", current_state.get("reported"))
                add("current.state.desired", current_state.get("desired"))

        previous = payload.get("previous")
        if isinstance(previous, dict):
            prev_state = previous.get("state")
            if isinstance(prev_state, dict):
                add("previous.state", prev_state)
                add("previous.state.reported", prev_state.get("reported"))
                add("previous.state.desired", prev_state.get("desired"))

        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            add("metadata", metadata)

        top_level_reported = payload.get("reported")
        if isinstance(top_level_reported, dict):
            add("reported", top_level_reported)

        return out

    def _derive_live_state_summary(self, state: dict[str, Any]) -> dict[str, Any]:
        cms = state.get("cleanMissionStatus") if isinstance(state.get("cleanMissionStatus"), dict) else {}
        pose = state.get("pose") if isinstance(state.get("pose"), dict) else {}
        point = pose.get("point") if isinstance(pose.get("point"), dict) else {}
        signal = state.get("signal") if isinstance(state.get("signal"), dict) else {}
        dock = state.get("dock") if isinstance(state.get("dock"), dict) else {}
        bin_state = state.get("bin") if isinstance(state.get("bin"), dict) else {}
        return {
            "batPct": state.get("batPct"),
            "phase": cms.get("phase"),
            "cycle": cms.get("cycle"),
            "error": cms.get("error"),
            "notReady": cms.get("notReady"),
            "mssnM": cms.get("mssnM"),
            "sqft": cms.get("sqft"),
            "x": point.get("x"),
            "y": point.get("y"),
            "theta": pose.get("theta"),
            "dock_known": dock.get("known"),
            "dock_contact": dock.get("contact"),
            "rssi": signal.get("rssi"),
            "snr": signal.get("snr"),
            "bin_present": bin_state.get("present"),
            "bin_full": bin_state.get("full"),
            "last_topic": ((state.get("_meta") or {}).get("last_topic") if isinstance(state.get("_meta"), dict) else None),
            "last_update": self._last_live_state_update,
        }

    async def _notify_live_state_listeners(self) -> None:
        snapshot = self.get_live_state_snapshot()
        for callback in list(self._live_state_listeners):
            try:
                result = callback(snapshot)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as err:
                _LOGGER.debug("roomba_v4 live state listener failed: %s", err)

    async def _persist_live_state_debug(self) -> None:
        path = self._event_debug_path("live_state.json")
        summary_path = self._event_debug_path("live_state_summary.json")
        if not path or not summary_path:
            return

        snapshot = self.get_live_state_snapshot()
        summary = self._derive_live_state_summary(snapshot)

        def _write() -> None:
            if self.debug_dir:
                self.debug_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
            summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

        await asyncio.to_thread(_write)

    async def _ingest_live_state_payload(self, topic: str | None, payload: Any) -> None:
        mode = self._topic_merge_mode(topic)
        source_kind = self._topic_source_kind(topic)
        if mode == "software":
            self._live_state.setdefault("_debug", {})["last_software_payload"] = payload if isinstance(payload, dict) else {"value": str(payload)}
            return

        fragments = self._extract_shadow_state_fragments(payload)
        livemap_fragment: dict[str, Any] = {}
        if mode == "livemap":
            livemap_fragment = self._extract_livemap_fragment(payload)
            if livemap_fragment and not fragments:
                fragments = [("livemap", livemap_fragment)]
        if not fragments:
            return
        if mode == "livemap" and livemap_fragment:
            await self._write_runtime_debug("livemap_parsed", {
                "topic": topic,
                "fragment": livemap_fragment,
                "payload": payload,
            })
        changed = False
        updates: list[dict[str, Any]] = []
        source_state = self._live_state_sources.setdefault(source_kind, {})
        for label, fragment in fragments:
            filtered = self._filter_fragment_for_topic(fragment, mode)
            if not filtered:
                continue
            before = json.dumps(source_state, sort_keys=True, default=str)
            self._deep_merge_dict(source_state, filtered)
            after = json.dumps(source_state, sort_keys=True, default=str)
            if before != after:
                changed = True
                updates.append({"label": label, "keys": sorted(filtered.keys())[:50], "mode": mode, "source": source_kind})

        if not changed:
            return

        self._last_live_state_update = datetime.now(tz=UTC).isoformat()
        self._live_state = self._compose_effective_live_state()
        self._live_state["_meta"] = {
            "last_topic": topic,
            "last_update": self._last_live_state_update,
            "updates": updates[-10:],
            "source_kind": source_kind,
            "has_currentstate_update": bool(self._live_state_sources.get("currentstate_update")),
        }
        await self._persist_live_state_debug()
        history_path = self._event_debug_path("live_state_updates.json")
        if history_path:
            def _append() -> None:
                items = []
                if history_path.exists():
                    try:
                        items.extend(json.loads(history_path.read_text(encoding="utf-8")))
                    except Exception:
                        items = []
                items.append({
                    "ts": self._last_live_state_update,
                    "topic": topic,
                    "updates": updates[-10:],
                    "summary": self._derive_live_state_summary(self._live_state),
                })
                items = items[-200:]
                history_path.write_text(json.dumps(items, indent=2, default=str), encoding="utf-8")
            await asyncio.to_thread(_append)
        await self._notify_live_state_listeners()

    async def _write_runtime_debug(self, stage: str, data: dict[str, Any]) -> str:
        path = "/tmp/roomba_v4_last_debug.json"
        if self.debug_dir:
            try:
                out = self.debug_dir / f"runtime_{stage}.json"
                payload = json.dumps(data, indent=2, default=str)
                def _write() -> str:
                    self.debug_dir.mkdir(parents=True, exist_ok=True)
                    out.write_text(payload, encoding="utf-8")
                    return str(out)
                path = await asyncio.to_thread(_write)
            except Exception as err:
                _LOGGER.debug("roomba_v4 runtime debug write failed stage=%s err=%s", stage, err)
        _LOGGER.debug("v111 debug snapshot stage=%s path=%s", stage, path)
        return path

    def _event_debug_path(self, filename: str) -> Path | None:
        if not self.debug_dir:
            return None
        return self.debug_dir / filename


    def _merge_live_state_from_packet(self, packet: dict[str, Any] | None) -> None:
        """Compat helper for older subscriber paths that expect a sync packet handler."""
        if not isinstance(packet, dict):
            return
        if packet.get("type") != 3:
            return
        payload = packet.get("payload_json")
        if not isinstance(payload, dict):
            return
        topic = packet.get("topic")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._ingest_live_state_payload(topic, payload))

    async def _append_event_packet(self, packet: dict[str, Any]) -> None:
        path = self._event_debug_path("mqtt_packets.json")
        last_path = self._event_debug_path("mqtt_last_packet.json")
        if not path or not last_path:
            return

        def _write() -> None:
            if self.debug_dir:
                self.debug_dir.mkdir(parents=True, exist_ok=True)
            packets = []
            if path.exists():
                try:
                    packets.extend(json.loads(path.read_text(encoding="utf-8")))
                except Exception:
                    packets = []
            packets.append(packet)
            packets = packets[-400:]
            path.write_text(json.dumps(packets, indent=2, default=str), encoding="utf-8")
            last_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")
            if packet.get("type") == "message" and isinstance(packet.get("payload_json"), dict):
                topic = str(packet.get("topic") or "")
                if "shadow" in topic:
                    snap = self._event_debug_path("mqtt_last_shadow_snapshot.json")
                    if snap:
                        snap.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")

        await asyncio.to_thread(_write)
        if packet.get("type") == "message":
            await self._ingest_live_state_payload(packet.get("topic"), packet.get("payload_json"))

    async def _log_event_status(self, payload: dict[str, Any]) -> None:
        await self._append_event_packet({
            "ts": datetime.now(tz=UTC).isoformat(),
            "type": "subscription_status",
            "topic": None,
            "payload_json": payload,
            "payload_text": None,
        })



    def _cloud_topic_sequences(self, robot_id: str) -> list[list[str]]:
        robot = self.robots.get(robot_id) or {}
        svc = str(robot.get("svcDeplId") or self.deployment.get("svcDeplId") or "v007")
        irbt = str(robot.get("irbtTopics") or self.deployment.get("irbtTopics") or f"{svc}-irbthbu")
        livemap_topic = self._livemap_topics.get(robot_id)
        primary_group = [f"$aws/things/{robot_id}/shadow/#"]
        if isinstance(livemap_topic, str) and livemap_topic:
            primary_group.append(livemap_topic)
            primary_group.append(f"{livemap_topic}/#")
        sequences: list[list[str]] = [
            primary_group,
            [f"$aws/things/{robot_id}/jobs/#"],
            [f"$aws/things/{robot_id}/shadow/name/{svc}/#"],
            [f"{svc}/things/{robot_id}/#"],
        ]
        if irbt and irbt != svc:
            sequences.extend([
                [f"$aws/things/{robot_id}/shadow/name/{irbt}/#"],
                [f"{irbt}/things/{robot_id}/#"],
            ])

        flat_topics = self._cloud_topic_filters(robot_id)
        if flat_topics:
            sequences.append(flat_topics)

        ordered: list[list[str]] = []
        seen: set[tuple[str, ...]] = set()
        for group in sequences:
            normalized = tuple(topic for topic in group if topic)
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(list(normalized))
        return ordered

    def _cloud_get_topics(self, robot_id: str) -> list[str]:
        preferred_names = [
            "ro-currentstate",
            "ro-stats",
            "ro-mapstate",
            "ro-missionstatus",
            "ro-preferences",
        ]
        # Do NOT include deployment-derived shadow names like v007 / v007-irbthbu here.
        # Earlier traces showed those startup GETs could destabilize the session, while the
        # useful live updates came from the ro-* named shadows below.
        topics = [f"$aws/things/{robot_id}/shadow/name/{name}/get" for name in preferred_names]
        topics.append(f"$aws/things/{robot_id}/shadow/get")
        return topics

    def _mqtt_pingreq_packet(self) -> bytes:
        return b"\xc0\x00"

    async def _capture_post_connack_idle(self, ws, *, seconds: float, phase: str) -> list[dict[str, Any]]:
        packets: list[dict[str, Any]] = []
        deadline = asyncio.get_running_loop().time() + seconds
        while asyncio.get_running_loop().time() < deadline and not self._subscriber_stop.is_set():
            remaining = max(0.05, deadline - asyncio.get_running_loop().time())
            try:
                packet = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=min(0.5, remaining))
            except asyncio.TimeoutError:
                continue
            packets.append(packet)
            await self._append_event_packet({
                "ts": datetime.now(tz=UTC).isoformat(),
                "type": "message" if packet.get("type") == 3 else f"packet_{packet.get('type')}",
                "topic": packet.get("topic"),
                "payload_json": {"phase": phase, **packet},
                "payload_text": packet.get("payload_text"),
            })
            self._merge_live_state_from_packet(packet)
        return packets

    async def _send_safe_shadow_gets(self, ws, *, robot_id: str, phase: str) -> list[dict[str, Any]]:
        sent: list[dict[str, Any]] = []
        for idx, topic in enumerate(self._cloud_get_topics(robot_id), start=1):
            if self._subscriber_stop.is_set():
                break
            payload = b"{}"
            publish_packet = self._mqtt_publish_packet(topic, payload)
            await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                "phase": "safe_shadow_trigger_sending",
                "packet_type": "PUBLISH",
                "topic": topic,
                "packet_hex": self._hex_bytes(publish_packet),
                "packet_len": len(publish_packet),
                "payload_text": "{}",
            })
            await ws.send(publish_packet)
            await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                "phase": "safe_shadow_trigger_send_completed",
                "packet_type": "PUBLISH",
                "topic": topic,
                "packet_hex": self._hex_bytes(publish_packet),
                "packet_len": len(publish_packet),
                "payload_text": "{}",
            })
            item = {
                "index": idx,
                "topic": topic,
                "payload": "{}",
                "phase": phase,
                "ts": datetime.now(tz=UTC).isoformat(),
            }
            sent.append(item)
            await self._append_event_packet({
                "ts": item["ts"],
                "type": "safe_shadow_get_sent",
                "topic": topic,
                "payload_json": {"phase": phase, "topic": topic, "payload": {}},
                "payload_text": "{}",
            })
            await asyncio.sleep(0.35)
        return sent

    async def _capture_safe_trigger_window(self, ws, *, robot_id: str) -> dict[str, Any]:
        await asyncio.sleep(0.5)
        sent = await self._send_safe_shadow_gets(ws, robot_id=robot_id, phase="safe_shadow_trigger")
        observed = await self._capture_post_connack_idle(ws, seconds=3.0, phase="post_safe_shadow_trigger")
        return {"sent": sent, "observed": observed[-40:]}

    async def _mqtt_wait_for_suback(self, ws, *, packet_id: int, topics: list[str], timeout: float, phase: str) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        observed: list[dict[str, Any]] = []
        while asyncio.get_running_loop().time() < deadline and not self._subscriber_stop.is_set():
            remaining = max(0.1, deadline - asyncio.get_running_loop().time())
            packet = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=remaining)
            ptype = packet.get("type")
            if ptype == 9 and packet.get("packet_id") == packet_id:
                return {"suback": packet, "observed": observed}
            observed.append(packet)
            await self._append_event_packet({
                "ts": datetime.now(tz=UTC).isoformat(),
                "type": "message" if ptype == 3 else f"packet_{ptype}",
                "topic": packet.get("topic"),
                "payload_json": {"phase": phase, **packet},
                "payload_text": packet.get("payload_text"),
            })
            self._merge_live_state_from_packet(packet)
        raise CloudApiError(f"timeout waiting for suback packet_id={packet_id} topics={topics}")

    def _cloud_topic_filters(self, robot_id: str) -> list[str]:
        robot = self.robots.get(robot_id) or {}
        svc = str(robot.get("svcDeplId") or self.deployment.get("svcDeplId") or "v007")
        irbt = str(robot.get("irbtTopics") or self.deployment.get("irbtTopics") or f"{svc}-irbthbu")
        topics = [
            f"$aws/things/{robot_id}/shadow/get/accepted",
            f"$aws/things/{robot_id}/shadow/update/documents",
            f"$aws/things/{robot_id}/shadow/update/accepted",
            f"$aws/things/{robot_id}/shadow/name/{svc}/get/accepted",
            f"$aws/things/{robot_id}/shadow/name/{svc}/update/documents",
            f"$aws/things/{robot_id}/shadow/name/{svc}/update/accepted",
        ]
        if irbt and irbt != svc:
            topics.extend([
                f"$aws/things/{robot_id}/shadow/name/{irbt}/get/accepted",
                f"$aws/things/{robot_id}/shadow/name/{irbt}/update/documents",
                f"$aws/things/{robot_id}/shadow/name/{irbt}/update/accepted",
            ])
        livemap_topic = self._livemap_topics.get(robot_id)
        if isinstance(livemap_topic, str) and livemap_topic:
            topics.append(livemap_topic)
            topics.append(f"{livemap_topic}/#")
        ordered: list[str] = []
        seen: set[str] = set()
        for topic in topics:
            if topic not in seen:
                seen.add(topic)
                ordered.append(topic)
        return ordered[:9]

    async def _probe_local_http_endpoints(self, robot_id: str, cycle: int) -> None:
        robot = self.robots.get(robot_id) or {}
        hosts = self._local_mqtt_host_candidates(robot_id)
        hosts = [h for h in hosts if h]
        if not hosts:
            return

        common_paths = [
            "/",
            "/status",
            "/info",
            "/mission",
            "/network",
            "/api/local/info",
            "/api/local/status",
            "/api/local/mission",
            "/api/local/network",
            "/api/v1/status",
            "/api/v1/mission",
            "/api/v1/network",
            "/umi",
            "/umi/mission",
            "/umi/network",
            "/asset/mission",
            "/asset/network",
            "/health",
        ]
        results: list[dict[str, Any]] = []
        timeout = aiohttp.ClientTimeout(total=6, connect=3, sock_read=4)
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        async def port_probe(host: str, port: int, use_ssl: bool) -> dict[str, Any]:
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=ssl_ctx if use_ssl else None, server_hostname=None if use_ssl else None), timeout=4)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return {"host": host, "port": port, "use_ssl": use_ssl, "open": True}
            except Exception as err:
                return {"host": host, "port": port, "use_ssl": use_ssl, "open": False, "error": str(err)}

        async with aiohttp.ClientSession(timeout=timeout) as probe_session:
            for host in hosts[:2]:
                for port, use_ssl in [(80, False), (443, True), (8080, False), (8443, True)]:
                    results.append(await port_probe(host, port, use_ssl))
                for scheme, port in [("http", 80), ("https", 443), ("http", 8080), ("https", 8443)]:
                    for p in common_paths:
                        url = f"{scheme}://{host}:{port}{p}"
                        item = {"host": host, "url": url, "method": "GET"}
                        try:
                            async with probe_session.get(url, ssl=ssl_ctx if scheme == "https" else None, allow_redirects=False) as resp:
                                body = await resp.text(errors="ignore")
                                item.update({
                                    "status": resp.status,
                                    "headers": dict(resp.headers),
                                    "body_prefix": body[:300],
                                })
                        except Exception as err:
                            item["error"] = str(err)
                        results.append(item)

        await self._write_runtime_debug("local_http_probe", {
            "cycle": cycle,
            "transport": "v104_local_http_probe",
            "robot_id": robot_id,
            "robot_sku": robot.get("sku"),
            "hosts": hosts,
            "results": results[:400],
        })

    async def async_shutdown_event_subscriber(self) -> None:
        self._subscriber_stop.set()
        self._subscriber_ready.clear()
        self._subscriber_ws = None
        self._subscriber_robot_id = None
        self._subscriber_topics_subscribed.clear()
        task = self._subscriber_task
        self._subscriber_task = None
        if task:
            task.cancel()
            try:
                await task
            except Exception:
                pass

    async def async_ensure_event_subscriber(self, robot_id: str) -> None:
        livemap_topic = None
        livemap_lookup_error = None
        priming_summary = None
        priming_error = None
        try:
            priming_summary = await self.prime_livemap_session(robot_id)
        except Exception as err:
            priming_error = str(err)
            await self._write_runtime_debug("p2maps_prime_error", {
                "robot_id": robot_id,
                "error": str(err),
            })
        try:
            livemap_topic = await self.get_livemap_mqtt_topic(robot_id)
        except Exception as err:
            livemap_lookup_error = str(err)
            await self._write_runtime_debug("livemap_topic_lookup_error", {
                "robot_id": robot_id,
                "error": str(err),
            })
        task = self._subscriber_task
        ws_usable = self._subscriber_ws_is_usable(robot_id=robot_id)
        livemap_topic_missing = bool(livemap_topic and livemap_topic not in self._subscriber_topics_subscribed)
        await self._write_runtime_debug("mqtt_start", {
            "robot_id": robot_id,
            "requested_at": datetime.now(tz=UTC).isoformat(),
            "task_exists": bool(task),
            "task_done": bool(task.done()) if task else None,
            "ws_usable": ws_usable,
            "livemap_topic": livemap_topic,
            "livemap_topic_lookup_error": livemap_lookup_error,
            "p2maps_priming_error": priming_error,
            "p2maps_priming_selected_p2map_id": (priming_summary or {}).get("selected_p2map_id") if isinstance(priming_summary, dict) else None,
            "p2maps_priming_selected_active_p2mapv_id": (priming_summary or {}).get("selected_active_p2mapv_id") if isinstance(priming_summary, dict) else None,
            "livemap_topic_missing_from_session": livemap_topic_missing,
            "subscribed_topics": sorted(self._subscriber_topics_subscribed),
            "mode": "v207_single_session_shadow_plus_livemap_with_p2maps_priming",
        })
        if task and not task.done() and ws_usable and not livemap_topic_missing:
            return
        if task and not task.done():
            await self._write_runtime_debug("mqtt_restart_triggered", {
                "robot_id": robot_id,
                "reason": ("livemap_topic_added" if livemap_topic_missing else "session_not_usable"),
                "livemap_topic": livemap_topic,
                "subscribed_topics": sorted(self._subscriber_topics_subscribed),
            })
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._subscriber_task = None
        self._subscriber_stop = asyncio.Event()
        self._subscriber_ready.clear()
        self._subscriber_ws = None
        self._subscriber_robot_id = None
        self._subscriber_topics_subscribed.clear()
        self._subscriber_task = asyncio.create_task(self._event_subscriber_loop(robot_id))

    def _subscriber_connection_candidates(self, *, endpoint: str, robot_id: str, region: str) -> list[dict[str, Any]]:
        _exact_query_variants, exact_header_variants, exact_client_ids = self._exact_query_and_header_variants(robot_id)
        preferred_client_ids = exact_client_ids + [
            f"app-IOS-{self.config['appId']}-{robot_id[:10]}",
            f"IOS-{self.config['appId']}",
            f"HA-{self.config['deviceId'][:12]}",
        ]
        client_ids = list(dict.fromkeys([cid for cid in preferred_client_ids if cid]))[:2]
        if not client_ids:
            client_ids = [f"app-IOS-{self.config['appId']}-{robot_id[:10]}"]

        header_map = {name: headers for name, headers in exact_header_variants + self._custom_authorizer_header_variants()}
        apk_headers = header_map.get("apk_exact_headers") or Headers()
        combos: list[tuple[str, str, Headers, str]] = [
            ("direct_mqtt_always_on_subscriber", f"wss://{endpoint}:443/mqtt", apk_headers, "apk_exact_headers"),
        ]

        out: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for url_name, url, headers, header_name in combos:
            for client_id in client_ids:
                key = (url_name, url, header_name, client_id)
                if key in seen:
                    continue
                seen.add(key)
                out.append({
                    "url_name": url_name,
                    "url": url,
                    "header_name": header_name,
                    "headers": headers,
                    "client_id": client_id,
                })
        return out

    def _subscriber_connect_variants(self, robot_id: str) -> list[dict[str, Any]]:
        # v121 single-slot mqtt auth mode: keep exactly one CONNECT shape per startup cycle,
        # but include MQTT auth fields using the robot password.
        robot = self.robots.get(robot_id) or {}
        mqtt_password = robot.get("password")
        if isinstance(mqtt_password, str) and mqtt_password:
            return [{"name": "robotid_password", "username": robot_id, "password": mqtt_password}]
        return [{"name": "client_id_only", "username": None, "password": None}]

    def _auth_debug_snapshot(self, candidate: dict[str, Any]) -> dict[str, Any]:
        url = candidate["url"]
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        headers = candidate.get("headers") or Headers()
        return {
            "client_id": candidate.get("client_id"),
            "url_name": candidate.get("url_name"),
            "header_name": candidate.get("header_name"),
            "base": f"{parsed.scheme}://{parsed.netloc}{parsed.path}",
            "query_keys": sorted(query.keys()),
            "query_preview": {k: ["<redacted>"] for k in sorted(query.keys())},
            "header_keys": sorted({k for k, _v in headers.raw_items()}),
        }

    async def _event_subscriber_loop(self, robot_id: str) -> None:
        await self._write_runtime_debug("mqtt_loop_entered", {
            "robot_id": robot_id,
            "entered_at": datetime.now(tz=UTC).isoformat(),
            "mode": "v144_post_command_shadow_refresh",
        })
        endpoint = self.deployment.get("mqttApp") or self.deployment.get("mqtt") or self.deployment.get("mqttAts")
        if not endpoint:
            await self._log_event_status({"status": "failed", "transport": "v144_post_command_shadow_refresh", "error": "missing endpoint"})
            return

        region = self.deployment.get("awsRegion") or self._region_from_endpoint(endpoint)
        ssl_context = await self._get_ssl_context()
        topics = self._cloud_topic_filters(robot_id)
        topic_sequences = self._cloud_topic_sequences(robot_id)[:1]

        while not self._subscriber_stop.is_set():
            self._subscriber_cycle += 1
            cycle = self._subscriber_cycle
            connected = False
            attempts: list[dict[str, Any]] = []

            candidates = self._subscriber_connection_candidates(endpoint=endpoint, robot_id=robot_id, region=region)
            await self._write_runtime_debug("subscriber_candidates", {
                "cycle": cycle,
                "endpoint": endpoint,
                "region": region,
                "topics": topics,
                "topic_sequences": topic_sequences,
                "mode": "post_command_shadow_refresh_mode",
                "candidates": [self._auth_debug_snapshot(candidate) for candidate in candidates],
            })

            connect_variants = self._subscriber_connect_variants(robot_id)
            if self._single_slot_safe_mode:
                candidates = candidates[:1]
            for candidate in candidates:
                if self._subscriber_stop.is_set():
                    break
                url = candidate["url"]
                extra_headers = candidate["headers"]
                client_id = candidate["client_id"]
                header_name = candidate["header_name"]
                for connect_variant in connect_variants:
                    if self._subscriber_stop.is_set():
                        break
                    mqtt_username = connect_variant.get("username")
                    mqtt_password = connect_variant.get("password")
                    attempt: dict[str, Any] = {
                        "client_id": client_id,
                        "url_variant": candidate["url_name"],
                        "header_variant": header_name,
                        "connect_variant": connect_variant.get("name"),
                        "mqtt_username": ("<empty>" if mqtt_username == "" else ("<set>" if mqtt_username is not None else None)),
                        "mqtt_password": ("<set>" if mqtt_password is not None else None),
                        "phase": "starting",
                        "started_at": datetime.now(tz=UTC).isoformat(),
                        "mode": "post_command_shadow_refresh_mode",
                    }
                    attempts.append(attempt)
                    candidate_with_auth = dict(candidate)
                    candidate_with_auth.update({
                        "connect_variant": connect_variant.get("name"),
                        "mqtt_username": mqtt_username,
                        "mqtt_password": mqtt_password,
                    })
                    await self._write_runtime_debug("auth_candidate", {
                        "cycle": cycle,
                        "robot_id": robot_id,
                        "candidate": self._auth_debug_snapshot(candidate_with_auth),
                    })
                    try:
                        await self._log_event_status({
                            "status": "starting",
                            "transport": "v144_post_command_shadow_refresh",
                            "endpoint": endpoint,
                            "port": 443,
                            "client_id": client_id,
                            "header_variant": header_name,
                            "url_variant": candidate["url_name"],
                            "cycle": cycle,
                        })
                        async with websockets.connect(
                            url,
                            subprotocols=["mqtt"],
                            user_agent_header=AWS_USER_AGENT,
                            additional_headers=extra_headers,
                            ssl=ssl_context,
                            open_timeout=20,
                            close_timeout=10,
                            max_size=2**20,
                            ping_interval=None,
                        ) as ws:
                            self._subscriber_ws = ws
                            self._subscriber_robot_id = robot_id
                            self._subscriber_ready.clear()
                            attempt["websocket_opened"] = True
                            attempt["phase"] = "mqtt_connect_sent"
                            attempt["connected_at"] = datetime.now(tz=UTC).isoformat()
                            connect_packet = self._mqtt_connect_packet(client_id, username=mqtt_username, password=mqtt_password, keepalive=300)
                            attempt["connect_hex"] = self._hex_bytes(connect_packet)
                            await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                                "cycle": cycle,
                                "client_id": client_id,
                                "url_variant": candidate["url_name"],
                                "header_variant": header_name,
                                "phase": "mqtt_connect_sent",
                                "packet_type": "CONNECT",
                                "connect_variant": connect_variant.get("name"),
                                "mqtt_username": ("<empty>" if mqtt_username == "" else ("<set>" if mqtt_username is not None else None)),
                                "mqtt_password": ("<set>" if mqtt_password is not None else None),
                                "packet_hex": self._hex_bytes(connect_packet),
                                "packet_len": len(connect_packet),
                            })
                            await ws.send(connect_packet)
                            attempt["connect_send_completed_at"] = datetime.now(tz=UTC).isoformat()
                            connack = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=6)
                            attempt["connack"] = connack
                            attempt["phase"] = "connack_received"
                            await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                                "cycle": cycle,
                                "client_id": client_id,
                                "url_variant": candidate["url_name"],
                                "header_variant": header_name,
                                "phase": "connack_received",
                                "packet_type": "CONNACK",
                                "packet_hex": connack.get("raw_hex"),
                                "packet_len": connack.get("raw_len"),
                                "packet": connack,
                            })
                            await self._append_event_packet({
                                "ts": datetime.now(tz=UTC).isoformat(),
                                "type": "connack",
                                "topic": None,
                                "payload_json": connack,
                                "payload_text": None,
                            })
                            if connack.get("type") != 2 or connack.get("return_code") not in {0, None}:
                                raise CloudApiError(f"bad connack: {connack}")
    
                            idle_packets: list[dict[str, Any]] = []
                            attempt["idle_packets"] = idle_packets[:10]
                            attempt["phase"] = "subscribe_sequence"
    
                            packet_id = 1
                            subscribe_results: list[dict[str, Any]] = []
                            subscribe_observed: list[dict[str, Any]] = []
                            for idx, group in enumerate(topic_sequences, start=1):
                                packet_id += 1
                                subscribe_packet = self._mqtt_subscribe_packet(packet_id, group, qos=1)
                                proof_payload = {
                                    "cycle": cycle,
                                    "client_id": client_id,
                                    "url_variant": candidate["url_name"],
                                    "header_variant": header_name,
                                    "connect_variant": connect_variant.get("name"),
                                    "phase": f"subscribe_group_{idx}_sending",
                                    "packet_type": "SUBSCRIBE",
                                    "packet_hex": self._hex_bytes(subscribe_packet),
                                    "packet_len": len(subscribe_packet),
                                    "packet_id": packet_id,
                                    "topics": group,
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                }
                                await self._write_runtime_debug("post_command_shadow_refresh_mode", proof_payload)
                                await self._write_packet_hex_trace("mqtt_packet_hex_trace", proof_payload)
                                await ws.send(subscribe_packet)
                                proof_payload = dict(proof_payload)
                                proof_payload.update({
                                    "phase": f"subscribe_group_{idx}_send_completed",
                                    "send_completed_at": datetime.now(tz=UTC).isoformat(),
                                })
                                await self._write_runtime_debug("post_command_shadow_refresh_mode", proof_payload)
                                await self._write_packet_hex_trace("mqtt_packet_hex_trace", proof_payload)
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "subscribe_sent",
                                    "topic": None,
                                    "payload_json": {"packet_id": packet_id, "topics": group, "stage": f"group_{idx}", "packet_hex": self._hex_bytes(subscribe_packet)},
                                    "payload_text": None,
                                })
                                result = await self._mqtt_wait_for_suback(
                                    ws,
                                    packet_id=packet_id,
                                    topics=group,
                                    timeout=8.0,
                                    phase=f"subscribe_wait_group_{idx}",
                                )
                                suback = result["suback"]
                                observed = result.get("observed") or []
                                subscribe_results.append({"topics": group, "suback": suback, "observed_count": len(observed)})
                                await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                                    "cycle": cycle,
                                    "client_id": client_id,
                                    "url_variant": candidate["url_name"],
                                    "header_variant": header_name,
                                    "phase": f"subscribe_group_{idx}_suback",
                                    "packet_type": "SUBACK",
                                    "packet_hex": suback.get("raw_hex"),
                                    "packet_len": suback.get("raw_len"),
                                    "packet_id": packet_id,
                                    "topics": group,
                                    "packet": suback,
                                })
                                subscribe_observed.extend(observed[:10])
                                await self._append_event_packet({
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "suback",
                                    "topic": None,
                                    "payload_json": {"packet_id": packet_id, "topics": group, "suback": suback},
                                    "payload_text": None,
                                })
                                if suback.get("type") != 9:
                                    raise CloudApiError(f"expected suback for {group}, got {suback}")
                                self._subscriber_topics_subscribed.update(topic for topic in group if topic)
                                await self._write_runtime_debug("subscriber_session_topics", {
                                    "cycle": cycle,
                                    "client_id": client_id,
                                    "url_variant": candidate["url_name"],
                                    "header_variant": header_name,
                                    "group_index": idx,
                                    "group_topics": group,
                                    "subscribed_topics": sorted(self._subscriber_topics_subscribed),
                                    "mode": "v202_single_session_shadow_plus_livemap",
                                })
                                await asyncio.sleep(1.0)
                            attempt["subscribe_results"] = subscribe_results[-10:]
                            attempt["subscribe_observed"] = subscribe_observed[-20:]
                            attempt["phase"] = "subscribed"
                            self._subscriber_ready.set()
    
                            await self._log_event_status({
                                "status": "subscribed",
                                "transport": "v144_post_command_shadow_refresh",
                                "endpoint": endpoint,
                                "port": 443,
                                "client_id": client_id,
                                "header_variant": header_name,
                                "url_variant": candidate["url_name"],
                                "connect_variant": connect_variant.get("name"),
                                "cycle": cycle,
                            })
    
                            topic_hits: dict[str, int] = {}
                            first_messages: list[dict[str, Any]] = []
                            warmup_packets = await self._capture_post_connack_idle(ws, seconds=2.0, phase="post_subscribe_warmup")
                            for packet in warmup_packets:
                                if packet.get("topic"):
                                    topic_hits[packet["topic"]] = topic_hits.get(packet["topic"], 0) + 1
                                if len(first_messages) < 25:
                                    payload_text = packet.get("payload_text") or ""
                                    first_messages.append({
                                        "type": "message" if packet.get("type") == 3 else f"packet_{packet.get('type')}",
                                        "topic": packet.get("topic"),
                                        "payload_text_prefix": payload_text[:512] if isinstance(payload_text, str) else None,
                                        "payload_json": packet.get("payload_json", packet),
                                    })
    
                            safe_trigger_result = None

                            await self._write_runtime_debug("cloud_topic_hits", {
                                "cycle": cycle,
                                "client_id": client_id,
                                "url_variant": candidate["url_name"],
                                "header_variant": header_name,
                                "topic_hits": topic_hits,
                            })
                            await self._write_runtime_debug("cloud_first_messages", {
                                "cycle": cycle,
                                "client_id": client_id,
                                "url_variant": candidate["url_name"],
                                "header_variant": header_name,
                                "messages": first_messages,
                            })
                            if safe_trigger_result is not None:
                                await self._write_runtime_debug("safe_shadow_trigger", {
                                    "cycle": cycle,
                                    "client_id": client_id,
                                    "url_variant": candidate["url_name"],
                                    "header_variant": header_name,
                                    "result": safe_trigger_result,
                                })
    
                            connected = True
                            attempt["phase"] = "steady_state"
                            attempt["steady_state_entered_at"] = datetime.now(tz=UTC).isoformat()
                            await self._write_runtime_debug("subscriber_connected", {
                                "cycle": cycle,
                                "client_id": client_id,
                                "url_variant": candidate["url_name"],
                                "header_variant": header_name,
                                "topics": topics,
                                "topic_sequences": topic_sequences,
                                "attempt": attempt,
                            })
    
                            idle_cycles = 0
                            bootstrap_sent = False
                            while not self._subscriber_stop.is_set():
                                try:
                                    packet = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=8)
                                    idle_cycles = 0
                                except asyncio.TimeoutError:
                                    idle_cycles += 1
                                    if not bootstrap_sent and not self._live_state:
                                        attempt["phase"] = "delayed_bootstrap_trigger"
                                        safe_trigger_result = await self._run_named_shadow_refresh_retry_window(
                                            robot_id,
                                            phase="startup_shadow_refresh",
                                            delays=(0.0, 2.0, 5.0, 10.0),
                                            capture_seconds=1.25,
                                        )
                                        bootstrap_sent = True
                                        attempt["safe_shadow_trigger"] = {
                                            "sent": safe_trigger_result.get("sent", [])[-10:],
                                            "observed_count": len(safe_trigger_result.get("observed", [])),
                                        }
                                        await self._write_runtime_debug("safe_shadow_trigger", {
                                            "cycle": cycle,
                                            "client_id": client_id,
                                            "url_variant": candidate["url_name"],
                                            "header_variant": header_name,
                                            "result": safe_trigger_result,
                                        })
                                        for packet in safe_trigger_result.get("observed", []):
                                            if packet.get("topic"):
                                                topic_hits[packet["topic"]] = topic_hits.get(packet["topic"], 0) + 1
                                        continue
                                    ping_packet = self._mqtt_pingreq_packet()
                                    await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                                        "cycle": cycle,
                                        "client_id": client_id,
                                        "url_variant": candidate["url_name"],
                                        "header_variant": header_name,
                                        "phase": "steady_state_pingreq_sending",
                                        "packet_type": "PINGREQ",
                                        "packet_hex": self._hex_bytes(ping_packet),
                                        "packet_len": len(ping_packet),
                                    })
                                    await ws.send(ping_packet)
                                    await self._append_event_packet({
                                        "ts": datetime.now(tz=UTC).isoformat(),
                                        "type": "pingreq_sent",
                                        "topic": None,
                                        "payload_json": {"idle_cycles": idle_cycles},
                                        "payload_text": None,
                                    })
                                    continue
                                ptype = packet.get("type")
                                rec = {
                                    "ts": datetime.now(tz=UTC).isoformat(),
                                    "type": "message" if ptype == 3 else f"packet_{ptype}",
                                    "topic": packet.get("topic"),
                                    "payload_json": packet.get("payload_json", packet),
                                    "payload_text": packet.get("payload_text"),
                                }
                                await self._append_event_packet(rec)
                                self._merge_live_state_from_packet(packet)
                            break
                    except asyncio.CancelledError:
                        raise
                    except Exception as err:
                        attempt["error"] = str(err)
                        attempt["phase"] = attempt.get("phase") or "failed"
                        close_code = getattr(err, "code", None)
                        close_reason = getattr(err, "reason", None)
                        if close_code is not None:
                            attempt["close_code"] = close_code
                        if close_reason is not None:
                            attempt["close_reason"] = close_reason
                        await self._log_event_status({
                            "status": "disconnected",
                            "transport": "v144_post_command_shadow_refresh",
                            "endpoint": endpoint,
                            "port": 443,
                            "client_id": client_id,
                            "header_variant": header_name,
                            "url_variant": candidate["url_name"],
                            "cycle": cycle,
                            "phase": attempt.get("phase"),
                            "error": str(err),
                            "close_code": close_code,
                            "close_reason": close_reason,
                        })
                        if self._subscriber_stop.is_set():
                            break
                        continue

            self._subscriber_ready.clear()
            self._subscriber_ws = None
            self._subscriber_topics_subscribed.clear()
            if self._subscriber_robot_id == robot_id:
                self._subscriber_robot_id = None
            await self._write_runtime_debug("subscriber_attempts", {"cycle": cycle, "connected": connected, "attempts": attempts[:100]})
            if self._subscriber_stop.is_set():
                break
            await asyncio.sleep(120 if not connected else 5)

    async def _publish_via_app_like_wss(

        self,
        *,
        endpoint: str,
        robot_id: str,
        topics: list[str],
        payload_variants: list[tuple[str, dict[str, Any]]],
        client_ids: list[str],
        response_topics: list[str],
    ) -> dict[str, Any] | None:
        _, header_variants, exact_client_ids = self._exact_query_and_header_variants(robot_id)
        app_like_headers = [(name, headers) for name, headers in header_variants if name == "apk_exact_headers"]
        if not app_like_headers:
            return None
        ssl_context = await self._get_ssl_context()
        url = f"wss://{endpoint}:443/mqtt"
        preferred_client_ids = exact_client_ids + [cid for cid in client_ids if cid not in exact_client_ids]
        for header_name, extra_headers in app_like_headers:
            for client_id in preferred_client_ids[:4]:
                conn_info = {"transport": "app_like_wss", "client_id": client_id, "url": url, "header_variant": header_name}
                _LOGGER.debug("v60 APK-like websocket attempt client_id=%s header_variant=%s", client_id, header_name)
                try:
                    async with websockets.connect(
                        url,
                        subprotocols=["mqtt"],
                        user_agent_header=AWS_USER_AGENT,
                        additional_headers=extra_headers,
                        ssl=ssl_context,
                        open_timeout=20,
                        close_timeout=10,
                        max_size=2**20,
                    ) as ws:
                        await ws.send(self._mqtt_connect_packet(client_id))
                        connack = await self._mqtt_read_packet(ws)
                        conn_info["connack"] = connack
                        if connack.get("type") != 2 or connack.get("return_code") not in {0, None}:
                            _LOGGER.debug("v60 APK-like websocket rejected client_id=%s connack=%s", client_id, connack)
                            continue
                        if response_topics:
                            _LOGGER.debug("v60 APK-like websocket skipping pre-publish subscribe client_id=%s response_topics=%s", client_id, response_topics[:8])
                        observed_packets = []
                        for topic in topics:
                            for variant_name, body in payload_variants:
                                payload_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
                                await ws.send(self._mqtt_publish_packet(topic, payload_bytes))
                                _LOGGER.debug("v60 APK-like websocket published client_id=%s topic=%s payload_variant=%s", client_id, topic, variant_name)
                                end_time = asyncio.get_running_loop().time() + 0.35
                                while asyncio.get_running_loop().time() < end_time:
                                    remaining = end_time - asyncio.get_running_loop().time()
                                    try:
                                        packet = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=max(0.1, remaining))
                                    except asyncio.TimeoutError:
                                        break
                                    except Exception as err:
                                        observed_packets.append({"error": repr(err), "during": variant_name})
                                        break
                                    observed_packets.append(packet)
                                    if packet.get("type") == 3:
                                        _LOGGER.debug("v60 APK-like websocket observed publish client_id=%s payload_variant=%s response_topic=%s payload=%s", client_id, variant_name, packet.get("topic"), packet.get("payload_text"))
                                    else:
                                        _LOGGER.debug("v60 APK-like websocket observed packet client_id=%s payload_variant=%s packet=%s", client_id, variant_name, packet)
                                if observed_packets:
                                    await self._write_runtime_debug("mqtt_observed_packets", {"client_id": client_id, "topic": topic, "payload_variant": variant_name, "observed_packets": observed_packets[-20:]})
                                for packet in reversed(observed_packets[-10:]):
                                    text = json.dumps(packet, separators=(",", ":"), ensure_ascii=False) if isinstance(packet, dict) else str(packet)
                                    if any(mark in text.lower() for mark in ["reject", "invalid", "error", "fail", "busy", "unsupported"]):
                                        return {
                                            "status": "published",
                                            "transport": "app_like_wss",
                                            "endpoint": endpoint,
                                            "client_id": client_id,
                                            "header_variant": header_name,
                                            "topic": topic,
                                            "payload_variant": variant_name,
                                            "observed_packets": observed_packets[-10:],
                                        }
                        return {
                            "status": "published",
                            "transport": "app_like_wss",
                            "endpoint": endpoint,
                            "client_id": client_id,
                            "header_variant": header_name,
                            "topic": topics[0] if topics else None,
                            "payload_variant": payload_variants[0][0] if payload_variants else None,
                            "observed_packets": observed_packets[-10:],
                        }
                except Exception as err:
                    _LOGGER.debug("v60 APK-like websocket failed client_id=%s header_variant=%s error=%r", client_id, header_name, err)
        return None

    async def _publish_via_aws_iot_mqtt(self, commanddef: dict[str, Any]) -> dict[str, Any]:
        if not self.credentials or not self.deployment:
            raise AuthenticationError("Not authenticated")

        robot_id = str(commanddef.get("robot_id") or "")
        if not robot_id:
            raise CloudApiError("Commanddef missing robot_id")

        endpoint = self.deployment.get("mqttApp") or self.deployment.get("mqtt") or self.deployment.get("mqttAts")
        if not isinstance(endpoint, str) or not endpoint:
            raise CloudApiError("Discovery response missing MQTT endpoint")

        region = self.deployment.get("awsRegion") or self.credentials["CognitoId"].split(":")[0]
        irbt_topics = self.deployment.get("irbtTopics") or f"{self.deployment.get('svcDeplId','v007')}-irbthbu"
        topics = [
            f"{irbt_topics}/things/{robot_id}/cmd",
            f"{irbt_topics}/things/{robot_id}/command",
            f"{irbt_topics}/things/{robot_id}/commands",
            f"$aws/things/{robot_id}/shadow/update",
            f"$aws/things/{robot_id}/shadow/name/{irbt_topics}/update",
        ]
        response_topics = [
            f"{irbt_topics}/things/{robot_id}/evt",
            f"{irbt_topics}/things/{robot_id}/event",
            f"{irbt_topics}/things/{robot_id}/status",
            f"{irbt_topics}/things/{robot_id}/state",
            f"{irbt_topics}/things/{robot_id}/cmd/accepted",
            f"{irbt_topics}/things/{robot_id}/cmd/rejected",
            f"{irbt_topics}/things/{robot_id}/command/accepted",
            f"{irbt_topics}/things/{robot_id}/command/rejected",
            f"$aws/things/{robot_id}/shadow/update/accepted",
            f"$aws/things/{robot_id}/shadow/update/rejected",
            f"$aws/things/{robot_id}/shadow/name/{irbt_topics}/update/accepted",
            f"$aws/things/{robot_id}/shadow/name/{irbt_topics}/update/rejected",
        ]
        payload_variants = self._build_mqtt_payload_variants(commanddef)
        if self._is_room_clean_commanddef(commanddef):
            topics, payload_variants = self._room_clean_single_publish_plan(commanddef, irbt_topics, robot_id)
            _LOGGER.debug("v60 room-clean single-shot selected robot_id=%s topic=%s payload_variant=%s region_schema=%s", robot_id, topics[0], payload_variants[0][0], commanddef.get("_room_region_schema"))
        else:
            by_name = {name: body for name, body in payload_variants}
            preferred = self._preferred_payload_order(commanddef)
            payload_variants = [(name, by_name[name]) for name in preferred if name in by_name]
        _LOGGER.debug("v60 MQTT publish start endpoint=%s robot_id=%s", endpoint, robot_id)
        startup_debug_path = await self._write_runtime_debug("mqtt_start", {
            "endpoint": endpoint,
            "robot_id": robot_id,
            "region": region,
            "topics": topics,
            "payload_variants": [name for name, _ in payload_variants],
            "room_clean_single_shot": self._is_room_clean_commanddef(commanddef),
            "exact_connection_token_bundle": {k: (v[:4] + "…" + v[-4:] if isinstance(v, str) and len(v) > 12 else "<redacted>") for k, v in self._exact_connection_token_bundle(robot_id).items()},
        })

        ssl_context = await self._get_ssl_context()
        url_variants: list[tuple[str, str]] = []
        path_variants = ["/mqtt"]
        exact_query_variants, exact_header_variants, exact_client_ids = self._exact_query_and_header_variants(robot_id)

        # Try exact bundle-derived auth first.
        for auth_name, auth_query in exact_query_variants:
            for path in path_variants:
                url_variants.append((
                    f"exact_{auth_name}_{path.strip('/').replace('/','_') or 'mqtt'}",
                    self._aws_iot_presigned_wss_url(endpoint, region=region, extra_query=auth_query, path=path),
                ))

        # Then broader heuristics.
        for path in path_variants:
            for include_port in (False, True):
                for host_header in (None, endpoint, f"{endpoint}:443"):
                    for unsigned in (False, True):
                        url_variants.append((
                            f"sigv4_path_{path.strip('/').replace('/','_') or 'mqtt'}_port_{include_port}_host_{'default' if host_header is None else host_header.replace(':','_')}_unsigned_{unsigned}",
                            self._aws_iot_presigned_wss_url(
                                endpoint,
                                region=region,
                                include_port_in_url=include_port,
                                host_header=host_header,
                                use_unsigned_payload=unsigned,
                                path=path,
                            ),
                        ))
        for auth_name, auth_query in self._connection_token_query_variants():
            url_variants.append((
                f"sigv4_plus_{auth_name}",
                self._aws_iot_presigned_wss_url(endpoint, region=region, extra_query=auth_query),
            ))
            for path in path_variants[1:]:
                url_variants.append((
                    f"sigv4_plus_{auth_name}_{path.strip('/').replace('/','_')}",
                    self._aws_iot_presigned_wss_url(endpoint, region=region, extra_query=auth_query, path=path),
                ))

        dedup_urls: list[tuple[str, str]] = []
        seen_urls: set[str] = set()
        for name, url in url_variants:
            if url not in seen_urls:
                seen_urls.add(url)
                dedup_urls.append((name, url))

        client_id_candidates = exact_client_ids + [
            f"IOS-{self.config['appId']}",
            f"HA-{self.config['deviceId'][:12]}",
            f"{robot_id[:10]}-{uuid.uuid4().hex[:8]}",
        ]
        client_id_candidates = list(dict.fromkeys(client_id_candidates))
        app_like_result = await self._publish_via_app_like_wss(
            endpoint=endpoint,
            robot_id=robot_id,
            topics=topics,
            payload_variants=payload_variants,
            client_ids=client_id_candidates,
            response_topics=response_topics,
        )
        if app_like_result:
            return app_like_result

        tls_result = await self._publish_via_tls_mqtt_custom_authorizer(
            endpoint=endpoint,
            robot_id=robot_id,
            topics=topics,
            payload_variants=payload_variants,
            client_ids=client_id_candidates,
        )
        if tls_result:
            return tls_result
        connection_attempts: list[dict[str, Any]] = []
        header_variants = exact_header_variants + self._custom_authorizer_header_variants()
        # Dedup headers by raw items
        _seen_header_keys: set[tuple[tuple[str, str], ...]] = set()
        _dedup_headers: list[tuple[str, Headers]] = []
        for name, headers in header_variants:
            items = tuple(sorted((k.lower(), v) for k, v in headers.raw_items()))
            if items not in _seen_header_keys:
                _seen_header_keys.add(items)
                _dedup_headers.append((name, headers))
        header_variants = _dedup_headers

        for url_name, wss_url in dedup_urls:
            for header_name, extra_headers in header_variants:
                for client_id in client_id_candidates:
                    conn_info: dict[str, Any] = {"client_id": client_id, "url_variant": url_name, "header_variant": header_name, **self._redact_url_query_keys(wss_url)}
                    _LOGGER.debug("v60 MQTT websocket attempt client_id=%s url_variant=%s header_variant=%s", client_id, url_name, header_name)
                    await self._write_runtime_debug("mqtt_attempt", conn_info)
                    try:
                        async with websockets.connect(
                            wss_url,
                            subprotocols=["mqtt"],
                            user_agent_header=AWS_USER_AGENT,
                            additional_headers=extra_headers,
                            ssl=ssl_context,
                            open_timeout=20,
                            close_timeout=10,
                            max_size=2**20,
                        ) as ws:
                            conn_info["websocket_opened"] = True
                            _LOGGER.debug("v60 MQTT websocket opened client_id=%s url_variant=%s header_variant=%s", client_id, url_name, header_name)
                            await self._write_runtime_debug("mqtt_opened", conn_info)
                            await ws.send(self._mqtt_connect_packet(client_id))
                            connack = await self._mqtt_read_packet(ws)
                            conn_info["connack"] = connack
                            if connack.get("type") != 2 or connack.get("return_code") not in {0, None}:
                                connection_attempts.append(conn_info)
                                continue
    
                            publish_attempts: list[dict[str, Any]] = []
                            for topic in topics:
                                for variant_name, body in payload_variants:
                                    payload_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
                                    attempt = {
                                        "topic": topic,
                                        "payload_variant": variant_name,
                                        "payload_preview": payload_bytes[:400].decode("utf-8", errors="replace"),
                                    }
                                    try:
                                        await ws.send(self._mqtt_publish_packet(topic, payload_bytes))
                                        attempt["published"] = True
                                        try:
                                            maybe_reply = await asyncio.wait_for(self._mqtt_read_packet(ws), timeout=0.25)
                                            attempt["broker_reply"] = maybe_reply
                                        except Exception:
                                            attempt["broker_reply"] = None
                                    except Exception as err:
                                        attempt["published"] = False
                                        attempt["error"] = str(err)
                                    publish_attempts.append(attempt)
                                    if attempt.get("published"):
                                        success = {
                                            "status": "published",
                                            "transport": "aws_iot_mqtt_wss",
                                            "endpoint": endpoint,
                                            "region": region,
                                            "wss_url_redacted": self._redact_url_query_keys(wss_url),
                                            "client_id": client_id,
                                            "topic": topic,
                                            "payload_variant": variant_name,
                                            "payload": body,
                                            "connack": connack,
                                            "publish_attempts": publish_attempts,
                                        }
                                        await self._write_runtime_debug("mqtt_success", success)
                                        _LOGGER.debug("v60 MQTT publish success topic=%s payload_variant=%s debug=%s", topic, variant_name, startup_debug_path)
                                        return success
                            conn_info["publish_attempts"] = publish_attempts
                    except Exception as err:
                        conn_info["error"] = str(err)
                        _LOGGER.debug("v60 MQTT websocket failed client_id=%s url_variant=%s header_variant=%s error=%s", client_id, url_name, header_name, err)
                        await self._write_runtime_debug("mqtt_failed_attempt", conn_info)
                    connection_attempts.append(conn_info)

        failure_payload = {'mqtt_endpoint': endpoint, 'region': region, 'topics': topics, 'client_ids': client_id_candidates, 'exact_connection_token_bundle': {k: (v[:4] + '…' + v[-4:] if isinstance(v, str) and len(v) > 12 else '<redacted>') for k, v in self._exact_connection_token_bundle(robot_id).items()}, 'url_variants': [name for name, _ in dedup_urls], 'connection_attempts': connection_attempts}
        debug_path = await self._write_runtime_debug("mqtt_failure", failure_payload)
        raise CloudApiError(
            "MQTT clean attempt failed. "
            f"Debug file: {debug_path}. "
            f"Debug: {json.dumps(failure_payload, separators=(',', ':'))}"
        )

    def _subscriber_ws_is_usable(self, *, robot_id: str | None = None) -> bool:
        ws = self._subscriber_ws
        if ws is None or getattr(ws, "closed", False):
            return False
        if robot_id and self._subscriber_robot_id and self._subscriber_robot_id != robot_id:
            return False
        task = self._subscriber_task
        if task is None or task.done():
            return False
        return self._subscriber_ready.is_set()

    async def _ensure_managed_command_session(self, robot_id: str, *, timeout: float = 20.0) -> None:
        if self._subscriber_ws_is_usable(robot_id=robot_id):
            return

        task = self._subscriber_task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._subscriber_task = None
            self._subscriber_ws = None
            self._subscriber_robot_id = None
            self._subscriber_ready.clear()

        await self.async_ensure_event_subscriber(robot_id)
        await asyncio.wait_for(self._subscriber_ready.wait(), timeout=timeout)
        if not self._subscriber_ws_is_usable(robot_id=robot_id):
            raise CloudApiError("managed MQTT subscriber session is not ready for command publish")

    async def _publish_via_existing_subscriber_session(self, commanddef: dict[str, Any]) -> dict[str, Any] | None:
        robot_id = str(commanddef.get("robot_id") or "")
        if not robot_id:
            return None
        try:
            await self._ensure_managed_command_session(robot_id)
        except Exception:
            return None
        ws = self._subscriber_ws
        if ws is None or getattr(ws, "closed", False):
            return None

        irbt_topics = self.deployment.get("irbtTopics") or f"{self.deployment.get('svcDeplId','v007')}-irbthbu"
        if self._is_room_clean_commanddef(commanddef):
            topics, payload_variants = self._room_clean_single_publish_plan(commanddef, irbt_topics, robot_id)
            topic = topics[0]
            variant_name, body = payload_variants[0]
        else:
            # APK + field evidence: selected-room cleaning is the only action that reaches
            # the robot consistently, and it uses the /cmd topic with a raw/localApp-style
            # payload first. Mirror that path for all command publishes before trying the
            # legacy /command topic/commanddefs ordering.
            topic = f"{irbt_topics}/things/{robot_id}/cmd"
            by_name = {name: body for name, body in self._build_mqtt_payload_variants(commanddef)}
            preferred = self._preferred_payload_order(commanddef)
            variant_name = next((name for name in preferred if name in by_name), None)
            if variant_name is None:
                return None
            body = by_name[variant_name]

        payload_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
        publish_packet = self._mqtt_publish_packet(topic, payload_bytes)
        async with self._subscriber_send_lock:
            if not self._subscriber_ws_is_usable(robot_id=robot_id):
                return None
            await self._write_runtime_debug("single_session_command", {
                "robot_id": robot_id,
                "topic": topic,
                "payload_variant": variant_name,
                "phase": "sending",
                "ts": datetime.now(tz=UTC).isoformat(),
            })
            await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                "phase": "single_session_command_sending",
                "packet_type": "PUBLISH",
                "topic": topic,
                "payload_variant": variant_name,
                "packet_hex": self._hex_bytes(publish_packet),
                "packet_len": len(publish_packet),
            })
            await self._subscriber_ws.send(publish_packet)
        return {
            "status": "published",
            "transport": "existing_subscriber_session",
            "topic": topic,
            "payload_variant": variant_name,
        }

    async def _send_post_command_shadow_refresh(self, robot_id: str) -> None:
        """Best-effort delayed refresh after commands.

        AWS IoT subscriber websockets may be gone by the time this deferred task runs.
        Never let this path raise or break a successful command publish.
        """
        ws = self._subscriber_ws
        if ws is None or getattr(ws, "closed", False) or self._subscriber_stop.is_set():
            await self._write_runtime_debug("single_session_command", {
                "robot_id": robot_id,
                "phase": "post_command_refresh_skipped",
                "reason": "subscriber_ws_unavailable",
                "ts": datetime.now(tz=UTC).isoformat(),
            })
            return
        try:
            await self._run_named_shadow_refresh_retry_window(
                robot_id,
                phase="post_command_shadow_refresh",
                delays=(3.0, 6.0),
                capture_seconds=1.5,
                include_generic_shadow=False,
            )
        except Exception as err:
            _LOGGER.warning(
                "v60 post-command refresh skipped after command publish robot_id=%s error=%s",
                robot_id,
                err,
            )
            await self._write_runtime_debug("single_session_command", {
                "robot_id": robot_id,
                "phase": "post_command_refresh_error",
                "error": str(err),
                "ts": datetime.now(tz=UTC).isoformat(),
            })

    async def _run_named_shadow_refresh_retry_window(
        self,
        robot_id: str,
        *,
        phase: str,
        delays: tuple[float, ...] = (0.0, 2.0, 5.0, 10.0),
        capture_seconds: float = 1.25,
        include_generic_shadow: bool = True,
    ) -> dict[str, Any]:
        sent: list[dict[str, Any]] = []
        observed: list[dict[str, Any]] = []
        refresh_topics = [
            topic for topic in self._cloud_get_topics(robot_id)
            if "/shadow/name/ro-currentstate/get" in topic or "/shadow/name/ro-stats/get" in topic or (include_generic_shadow and topic.endswith('/shadow/get'))
        ]
        if not refresh_topics:
            return {"sent": sent, "observed": observed}
        for attempt_index, delay in enumerate(delays, start=1):
            if self._subscriber_stop.is_set():
                break
            if delay > 0:
                await asyncio.sleep(delay)
            ws = self._subscriber_ws
            if ws is None or getattr(ws, "closed", False):
                break
            await self._write_runtime_debug(phase, {
                "robot_id": robot_id,
                "phase": "retry_window_attempt",
                "attempt_index": attempt_index,
                "delay": delay,
                "ts": datetime.now(tz=UTC).isoformat(),
            })
            for topic in refresh_topics:
                publish_packet = self._mqtt_publish_packet(topic, b"{}")
                async with self._subscriber_send_lock:
                    if self._subscriber_ws is None or getattr(self._subscriber_ws, "closed", False):
                        return {"sent": sent, "observed": observed}
                    await self._write_runtime_debug(phase, {
                        "robot_id": robot_id,
                        "topic": topic,
                        "phase": "sending",
                        "attempt_index": attempt_index,
                        "ts": datetime.now(tz=UTC).isoformat(),
                    })
                    await self._write_packet_hex_trace("mqtt_packet_hex_trace", {
                        "phase": f"{phase}_sending",
                        "packet_type": "PUBLISH",
                        "topic": topic,
                        "packet_hex": self._hex_bytes(publish_packet),
                        "packet_len": len(publish_packet),
                    })
                    await self._subscriber_ws.send(publish_packet)
                sent.append({
                    "attempt_index": attempt_index,
                    "topic": topic,
                    "ts": datetime.now(tz=UTC).isoformat(),
                })
                await asyncio.sleep(0.1)
            observed.extend(await self._capture_post_connack_idle(ws, seconds=capture_seconds, phase=phase))
        return {"sent": sent[-20:], "observed": observed[-40:]}

    async def publish_commanddef_via_cloud_mqtt(self, commanddef: dict[str, Any]) -> dict[str, Any]:
        robot_id = str(commanddef.get("robot_id") or "")
        _LOGGER.debug("v60 publish_commanddef_via_cloud_mqtt entered robot_id=%s command=%s", robot_id, commanddef.get("command"))
        async with self._command_send_lock:
            if robot_id:
                await self._write_runtime_debug("single_session_command", {
                    "robot_id": robot_id,
                    "phase": "single_managed_session_publish_start",
                    "ts": datetime.now(tz=UTC).isoformat(),
                })
                await self._ensure_managed_command_session(robot_id)

            last_error: Exception | None = None
            for attempt_no in (1, 2):
                try:
                    result = await self._publish_via_existing_subscriber_session(commanddef)
                    if result:
                        _LOGGER.debug("v60 publish_commanddef_via_cloud_mqtt completed status=%s transport=%s", result.get("status"), result.get("transport"))
                        if robot_id:
                            await self._write_runtime_debug("single_session_command", {
                                "robot_id": robot_id,
                                "phase": "single_managed_session_publish_ok",
                                "attempt": attempt_no,
                                "transport": result.get("transport"),
                                "ts": datetime.now(tz=UTC).isoformat(),
                            })
                        return result
                    raise CloudApiError("existing subscriber session unavailable")
                except Exception as err:
                    last_error = err
                    if robot_id:
                        await self._write_runtime_debug("single_session_command", {
                            "robot_id": robot_id,
                            "phase": "single_managed_session_publish_retry",
                            "attempt": attempt_no,
                            "error": str(err),
                            "ts": datetime.now(tz=UTC).isoformat(),
                        })
                    self._subscriber_ready.clear()
                    self._subscriber_ws = None
                    if attempt_no == 1 and robot_id:
                        await self.async_ensure_event_subscriber(robot_id)
                        await asyncio.wait_for(self._subscriber_ready.wait(), timeout=20.0)
                        continue
                    break
            raise CloudApiError(f"Managed MQTT command publish failed: {last_error}")

    async def async_send_simple_command(self, robot_id: str, command: str) -> dict[str, Any]:
        commanddef = {"robot_id": robot_id, "command": command, "params": {}}
        return await self.publish_commanddef_via_cloud_mqtt(commanddef)

    def get_cloud_transport_debug_info(self, robot_id: str) -> dict[str, Any]:
        login = self.last_login_response or {}
        robots = login.get("robots") if isinstance(login.get("robots"), dict) else {}
        robot = robots.get(robot_id) if isinstance(robots, dict) else {}
        creds = self.credentials or {}
        deployment = self.deployment or {}

        redacted_creds = {}
        if isinstance(creds, dict):
            for key, value in creds.items():
                if key in {"AccessKeyId", "SecretKey", "SessionToken"}:
                    if isinstance(value, str):
                        redacted_creds[key] = value[:6] + "…" + value[-4:] if len(value) > 12 else "<redacted>"
                    else:
                        redacted_creds[key] = "<redacted>"
                else:
                    redacted_creds[key] = value

        mqtt_candidates = {}
        if isinstance(robot, dict):
            for key, value in robot.items():
                if "mqtt" in key.lower() or "iot" in key.lower() or key.lower() in {"password", "blid", "sku", "softwarever", "svcdeplid"}:
                    mqtt_candidates[key] = value

        if isinstance(deployment, dict):
            for key, value in deployment.items():
                if "mqtt" in key.lower() or "iot" in key.lower() or "httpbase" in key.lower():
                    mqtt_candidates[f"deployment.{key}"] = value

        connection_tokens = login.get("connection_tokens") if isinstance(login, dict) else None
        redacted_connection_tokens = None
        if isinstance(connection_tokens, dict):
            redacted_connection_tokens = {k: ("<redacted>" if isinstance(v, str) else v) for k, v in connection_tokens.items()}

        exact_bundle = self._exact_connection_token_bundle(robot_id)
        redacted_exact_bundle = {k: (v[:4] + "…" + v[-4:] if isinstance(v, str) and len(v) > 12 else "<redacted>") for k, v in exact_bundle.items()}
        return {
            "robot_id": robot_id,
            "deployment": deployment,
            "credentials": redacted_creds,
            "robot_mqtt_candidates": mqtt_candidates,
            "connection_tokens": redacted_connection_tokens,
            "exact_connection_token_bundle": redacted_exact_bundle,
            "login_response_keys": sorted(login.keys()) if isinstance(login, dict) else [],
            "discovery_url_used": (self.discovered_endpoints or {}).get("_discovery_url_used"),
            "gigya_account_info_keys": sorted((self.gigya_account_info or {}).keys()),
        }
