# Ace Archive Keeper Client

A tool for participants to host backups of Ace Archive

## Installation

Pre‑built packages for Linux, macOS, and Windows are available on the project’s [Releases page](./-/releases).

### Linux

Download the .deb and install via:

```
sudo apt install keeper_0.3.8_amd64.deb
```
This will place the cli in /usr/bin/keeper and the GUI in /usr/lib/keeper/Keeper. If the `keeper` command does not show up in your path, ensure /usr/bin is on your path. The Keeper GUI should register itself with most desktop managers.

For non-Debian-based linux distributions download the Keeper-Linux-0.3.8.zip.
This contains standalone versions of both the CLI and the GUI.

### macOS

Download the .dmg from the [Releases page](./-/releases).

Open it and copy the Keeper.app into your Applications folder.

For a standalone copy of the macOS CLI, download the Darwin .zip.

#### Troubleshooting macOS app
macOS may block this app from running depending on your Gatekeeper settings. If the app fails to launch try the following:

1. Right‑click (or Control‑click) on Keeper.app in your Application folder and choose Open.
2. macOS will warn that the app is from an unidentified developer.
3. Click Open again to confirm.

If that fails try:

1. Open System Settings.
2. Navigate to Privacy & Security.
3. Scroll down to the Security section.
4. You should see a message saying the app was blocked from opening.
5. Click “Allow Anyway”.
6. Return to your Applications folder and  Right‑click (or Control‑click) on the app again and choose open.
7. When prompted, click Open again to confirm.

### Windows

Download the Windows .zip file from the [Releases page](./-/releases) and extract it wherever.

Double click keeper-Windows_NT-0.3.8 to start the GUI.

Run keeper-cli-Windows_NT-0.3.8 via command prompt to use the CLI.


## Usage

### GUI
Choose an output file and click 'GO'.

To cancel click 'STOP'.

It may take a few seconds to stop after canceling the backup or closing the app. This is normal.

For a full explanation of each of the fields, see bellow. 

#### Destination File
Use the **Pick File** button to select a destination file for the backup. If an existing .zip file is given an incremental backup will be attempted. Otherwise, a full backup will be performed.

If a file other than a .zip is selected or the .zip doesn't look like an Ace Archive Keeper backup, the existing file will be moved aside to prevent overwriting.

#### ID
A randomly generated unique identifier.

Once a backup is completed the Keeper tool reports the backup to [acearchive.lgbt](https://acearchive.lgbt/). This is so the Ace Archive maintainers can identify how many unique machines hold a copy of the archive.

The ID randomly generated not based on any machine identifier.

#### Email
Optionally associate this backup run to an email address.

This is only used in the event of disaster recovery. In the event the original archive is destroyed volunteer to be reached-out to to assist with rebuilding it from your backup.

#### Log Verbose
Print too much information about the backup progress.

#### Log File
Optionally log the backup progress to a file in addition to the output window.

### CLI
See `-h` for usage instructions:

```
$ keeper -h

usage: keeper [-h] [-e EMAIL] [-l LOG_FILE] [-v] [-q] [--config-file CONFIG_FILE] [-g] archive-zip

A tool to assist with incremental backup the Ace Archive.

positional arguments:
  archive-zip           Path to the output backup zip. If a zip file already exists at that location, performs an incremental backup. Otherwise performs a full backup.

options:
  -h, --help            show this help message and exit
  -e, --email EMAIL     Optionally provide an email address. This will only be used in a disaster recovery event. Not required.
  -l, --log-file LOG_FILE
                        Log what's happening to the specified file. Defaults to no log file.
  -v, --verbose         Increase logging verbosity to both the log file and stderr.
  -q, --quiet           Do not print log messages to stderr.
  --config-file CONFIG_FILE
                        Override the default config file. Should not generally be used.
  -g, --gui             Start the GUI. All other arguments will be ignored.

To get started, specify an output file after this command like: keeper keeper.zip'
```
