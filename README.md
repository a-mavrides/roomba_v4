# Roomba v4 - Home Assistant Integration

Custom Home Assistant integration for controlling and monitoring **iRobot Roomba** devices via the cloud API.

---

## ✨ Features

* Start / pause / stop cleaning
* Return to dock
* View cleaning status
* Battery level monitoring
* Room / mission support *(if supported by your model)*
* Cloud-based communication (no local setup required)

---

## 📦 Installation

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?repository=https://github.com/a-mavrides/roomba_v4&category=integration)

### Option 1: HACS (Recommended)

1. Open **HACS**
2. Click the **3-dot menu** → *Custom repositories*
3. Add this repository:

   ```
   https://github.com/a-mavrides/roomba_v4
   ```
4. Category: **Integration**
5. Click **Install**
6. Restart Home Assistant

---

### Option 2: Manual Installation

1. Download the latest release from GitHub

2. Extract the contents

3. Copy the folder:

   ```
   custom_components/roomba_v4
   ```

   into your Home Assistant `custom_components` directory

4. Restart Home Assistant

---

## ⚙️ Configuration

After installation:

1. Go to **Settings → Devices & Services**
2. Click **Add Integration**
3. Search for **Roomba v4**
4. Enter your iRobot account credentials

---

## 🔐 Authentication

This integration uses your **iRobot cloud account**.

Your credentials are only used to authenticate with iRobot services and are stored securely by Home Assistant.

---

## 🧰 Supported Devices

* iRobot Roomba models with cloud connectivity

> Note: Feature availability may vary depending on your Roomba model.

---

## 🐞 Troubleshooting

* Ensure your Roomba is connected to the internet
* Verify your iRobot account credentials
* Check Home Assistant logs:

  ```
  Settings → System → Logs
  ```

---

## 📜 Logs & Debugging

To enable debug logging, add the following to your `configuration.yaml`:

```yaml
logger:
  logs:
    custom_components.roomba_v4: debug
```

---

## 🤝 Contributing

Contributions are welcome!

* Fork the repository
* Create a feature branch
* Submit a pull request

---

## 🐛 Issues

If you encounter any problems, please open an issue:

👉 https://github.com/a-mavrides/roomba_v4/issues

---

## 📄 License

This project is licensed under the MIT License.

---

## ⚠️ Disclaimer

This is a **custom integration** and is not affiliated with or endorsed by iRobot.
