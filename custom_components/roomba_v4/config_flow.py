
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_create_clientsession

from .cloud_api import AuthenticationError, CloudApiError, IRobotCloudApi
from .const import CONF_AUTO_DOWNLOAD_MAP, CONF_COUNTRY_CODE, CONF_DEBUG_ENABLED, CONF_ROBOT_BLID, DEFAULT_COUNTRY_CODE, DOMAIN

_LOGGER = logging.getLogger(__name__)



class RoombaV4OptionsFlow(config_entries.OptionsFlowWithConfigEntry):

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        schema = vol.Schema({
            vol.Optional(
                CONF_AUTO_DOWNLOAD_MAP,
                default=self.config_entry.options.get(
                    CONF_AUTO_DOWNLOAD_MAP,
                    self.config_entry.data.get(CONF_AUTO_DOWNLOAD_MAP, True),
                ),
            ): bool,
            vol.Optional(
                CONF_DEBUG_ENABLED,
                default=self.config_entry.options.get(
                    CONF_DEBUG_ENABLED,
                    self.config_entry.data.get(CONF_DEBUG_ENABLED, False),
                ),
            ): bool,
        })
        return self.async_show_form(step_id="init", data_schema=schema)

class RoombaV4ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    @staticmethod
    @config_entries.callback
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> RoombaV4OptionsFlow:
        return RoombaV4OptionsFlow(config_entry)

    def __init__(self) -> None:
        self._user_input: dict[str, Any] = {}
        self._robots: dict[str, dict[str, Any]] = {}

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            self._user_input = user_input
            session = async_create_clientsession(self.hass)
            api = IRobotCloudApi(
                username=user_input[CONF_USERNAME],
                password=user_input[CONF_PASSWORD],
                country_code=user_input.get(CONF_COUNTRY_CODE, DEFAULT_COUNTRY_CODE),
                session=session,
            )
            try:
                await api.authenticate()
                self._robots = api.robots
                if not self._robots:
                    errors["base"] = "no_robots"
                elif len(self._robots) == 1:
                    return await self._create_entry(next(iter(self._robots)))
                return await self.async_step_select_robot()
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except CloudApiError as err:
                _LOGGER.exception("Cloud API error during setup: %s", err)
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during setup")
                errors["base"] = "unknown"

        schema = vol.Schema({
            vol.Required(CONF_USERNAME): str,
            vol.Required(CONF_PASSWORD): str,
            vol.Optional(CONF_COUNTRY_CODE, default=DEFAULT_COUNTRY_CODE): str,
            vol.Optional(CONF_AUTO_DOWNLOAD_MAP, default=True): bool,
        })
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_select_robot(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            return await self._create_entry(user_input[CONF_ROBOT_BLID])
        robot_options = {}
        for blid, data in self._robots.items():
            name = data.get("robotName") or data.get("name") or f"Roomba {blid[-6:]}"
            sku = data.get("sku", "")
            robot_options[blid] = f"{name} ({sku})"
        schema = vol.Schema({vol.Required(CONF_ROBOT_BLID): vol.In(robot_options)})
        return self.async_show_form(step_id="select_robot", data_schema=schema)

    async def _create_entry(self, blid: str) -> FlowResult:
        await self.async_set_unique_id(blid)
        self._abort_if_unique_id_configured()
        robot = self._robots.get(blid, {})
        title = robot.get("robotName") or robot.get("name") or f"Roomba {blid[-6:]}"
        return self.async_create_entry(title=title, data={**self._user_input, CONF_ROBOT_BLID: blid})
