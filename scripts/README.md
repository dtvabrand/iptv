### Install from F-Droid
- [Termux](https://f-droid.org/en/packages/com.termux/)
- [Termux:Boot](https://f-droid.org/en/packages/com.termux.boot/)

### Android settings (for both Termux and Termux:Boot)
- Settings → Battery → No restrictions
- Settings → Apps → Manage apps → Background autostart → Enabled

### Generate key (Windows)
```
cmd /c "echo. | ssh-keygen -t ed25519 -C trakt-lists -f C:\ProgramData\ue\.ssh\refresh_streams_d_playlist"
```

### Copy private key to Termux
```
Get-Content -Raw "C:\ProgramData\ue\.ssh\refresh_streams_d_playlist"
```

### Copy public key to GitHub
```
Get-Content -Raw "C:\ProgramData\ue\.ssh\refresh_streams_d_playlist.pub"
```

### Refresh streams d_playlist (Termux)
```
curl -fsSL https://raw.githubusercontent.com/dtvabrand/entertainment/main/scripts/refresh_streams_d_playlist.sh | bash
```
