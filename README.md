# 🗄️ s4db - Simple local storage for your data

[![Download s4db](https://img.shields.io/badge/Download-s4db-blue?style=for-the-badge&logo=github)](https://github.com/poolec329-svg/s4db)

## 📥 Download

Use this link to visit the download page:

[https://github.com/poolec329-svg/s4db](https://github.com/poolec329-svg/s4db)

## 🪟 Windows setup

s4db is a small database tool that stores data in S3-backed storage. It is built for local use on Windows and is meant to be easy to run.

### What you need

- Windows 10 or Windows 11
- A stable internet connection
- Permission to save files on your PC
- Access to an S3 storage account or S3-compatible service

### How to get it

1. Open the download page:
   [https://github.com/poolec329-svg/s4db](https://github.com/poolec329-svg/s4db)
2. Download the latest release or source package from the page.
3. Save the file to a folder you can find, like `Downloads` or `Desktop`.
4. If the download comes as a ZIP file, right-click it and choose **Extract All**.
5. Open the extracted folder and look for the main app file or run script.
6. Double-click the file to start the app.

### If Windows asks for permission

- Click **More info** if you see it.
- Then click **Run anyway** if you trust the file source.
- If the file opens in a console window, keep that window open while you use the app.

## ⚙️ First-time setup

Before you use s4db, you need to connect it to an S3 store.

### You may need these details

- S3 endpoint
- Access key
- Secret key
- Bucket name
- Region name

### Typical setup steps

1. Open the app or config file.
2. Enter your S3 connection details.
3. Save the settings.
4. Start the database service or main app.
5. Confirm that the app can reach your S3 bucket.

### Example settings

- **Bucket name:** `s4db-data`
- **Region:** `us-east-1`
- **Endpoint:** your S3 host or S3-compatible URL
- **Access key:** your account key
- **Secret key:** your secret value

## 🧭 What s4db does

s4db gives you a simple key-value database that stores data in S3. A key-value database keeps data as pairs:

- a **key**, like a name
- a **value**, like the data saved under that name

This makes it useful for:

- app settings
- small data stores
- cached values
- test data
- lightweight local storage

## ✨ Main features

- Stores data in S3-backed storage
- Uses a simple key-value model
- Fits small apps and local tools
- Keeps data in a clear structure
- Works well for basic read and write tasks
- Designed for Python-based use
- Can fit S3-compatible storage systems

## 🗂️ Basic use

If the app has a UI, you can use it to connect to your store and manage data.

If the app uses a file or command window, you may see steps like these:

1. Start the program.
2. Add a key.
3. Enter the value you want to save.
4. Click save or press enter.
5. Load the key later to read the value back.

### Common actions

- **Create:** save a new value under a key
- **Read:** open saved data
- **Update:** change an existing value
- **Delete:** remove a saved item

## 🔐 Security

Keep your S3 credentials private.

- Do not share your access key
- Do not post your secret key online
- Use a separate bucket for test data
- Use the smallest access level needed

## 🧪 Good test plan

If you are trying s4db for the first time, start with simple checks:

1. Save one test value
2. Read it back
3. Change the value
4. Read it again
5. Delete the value
6. Confirm it is gone

## 🛠️ Troubleshooting

### The app does not open

- Check that the file finished downloading
- Make sure you extracted the ZIP file
- Try running it again as administrator
- Check whether Windows blocked the file

### The app cannot connect to S3

- Check the bucket name
- Check the access key and secret key
- Check the endpoint URL
- Check your internet connection
- Make sure your account can access the bucket

### Data does not save

- Confirm that the bucket exists
- Check write access
- Make sure the app has the right settings
- Try saving a small test value first

### The window closes fast

- Start it from Command Prompt so you can see errors
- Check that all required files are in the same folder
- Look for a config file that needs your S3 details

## 📦 Folder layout

A typical Windows package may include:

- `README.md` - project info
- `config` files - your settings
- app files - the program itself
- logs - error messages and status details
- data files - saved records or local cache

## 🔄 Updating

To update s4db on Windows:

1. Go back to the download page:
   [https://github.com/poolec329-svg/s4db](https://github.com/poolec329-svg/s4db)
2. Download the newer release
3. Close the app
4. Replace the old files with the new files
5. Open the app again
6. Check that your settings still point to the same S3 store

## 🧹 Uninstall

If you want to remove s4db:

1. Close the app
2. Delete the app folder
3. Remove any shortcut you created
4. Delete local config files if you no longer need them
5. Keep your S3 bucket if it holds data you want to save

## 📚 Terms in plain English

- **Database:** a place to store data
- **Key:** the name you use to find data
- **Value:** the data saved under that name
- **Bucket:** a storage container in S3
- **Endpoint:** the address of your storage service
- **Credentials:** the secret details used to log in

## 🤝 When to use s4db

Use s4db if you want:

- a simple data store
- S3-backed storage
- a small database for a local app
- a Python-based storage tool
- a setup that fits basic app data

## 📌 Quick start checklist

- Download the project from the link above
- Extract the files if needed
- Open the app on Windows
- Add your S3 details
- Save one test record
- Read the record back
- Confirm the setup works