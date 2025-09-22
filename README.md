# 🎵 spotify-dl

A command line utility to download songs, podcasts, playlists and albums directly from Spotify.

> [!IMPORTANT]
> A Spotify Premium account is required.

> [!CAUTION]
> Usage of this software may infringe Spotify's terms of service or your local legislation. Use it under your own risk.

## 🚀 Features

- Download individual tracks, podcasts, playlists or full albums.
- Built with Rust for speed and efficiency.
- Supports metadata tagging and organized file output.
- Customizable failure backoff to respect Spotify rate limits when retries happen.

## ⚙️ Installation

You can install it using `cargo`, `homebrew`, from source or using a pre-built binary from the releases page.

### From crates.io using `cargo`

```
cargo install spotify-dl
```

### Using homebrew (macOs)

```
brew tap guillemcastro/spotify-dl
brew install spotify-dl
```

### From source

```
cargo install --git https://github.com/GuillemCastro/spotify-dl.git
```

## 🧭 Usage

```
spotify-dl 0.9.4-z-er
A commandline utility to download music directly from Spotify

USAGE:
    spotify-dl.exe [FLAGS] [OPTIONS] <tracks>...

FLAGS:
    -F, --force      Force download even if the file already exists
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -d, --destination <destination>    The directory where the songs will be downloaded
    -f, --format <format>              The format to download the tracks in. Default is flac. [default: flac]
    -t, --parallel <parallel>          Number of parallel downloads. Default is 5. [default: 5]
        --failure-delay-ms <ms>        Base delay in milliseconds to wait after a failure [default: 0]
        --failure-delay-multiplier <x> Multiplier applied to the delay for consecutive failures [default: 2]
        --failure-delay-max-ms <ms>    Cap on backoff delay in milliseconds [default: 60000]

ARGS:
    <tracks>...    A list of Spotify URIs or URLs (songs, podcasts, playlists or albums)
```

Songs, playlists and albums must be passed as Spotify URIs or URLs (e.g. `spotify:track:123456789abcdefghABCDEF` for songs and `spotify:playlist:123456789abcdefghABCDEF` for playlists or `https://open.spotify.com/playlist/123456789abcdefghABCDEF?si=1234567890`).

## ⏱️ Rate Limiting Backoff

Network hiccups or Spotify throttling can cause occasional download failures. The new failure backoff controls let you slow the downloader after a miss so retried tracks succeed more reliably:

- `--failure-delay-ms`: start delay applied after the first failure (for example `2000` for a 2s pause).
- `--failure-delay-multiplier`: growth factor for consecutive failures (e.g. `2.0` doubles each time).
- `--failure-delay-max-ms`: maximum delay cap so exponential growth does not run away.

When a backoff is triggered you will see a `[rate-limit]` message in the log and the next track waits before starting. Pair this with the GUI’s adaptive queue controls to keep longer sessions stable.

## 📋 Examples

- Download a single track:
```bash
spotify-dl https://open.spotify.com/track/TRACK_ID
```

- Download a playlist:

```
spotify-dl -u YOUR_USER -p YOUR_PASS https://open.spotify.com/playlist/PLAYLIST_ID
```

Save as MP3 to a custom folder:
```
spotify-dl --format flac --destination ~/Music/Spotify https://open.spotify.com/album/ALBUM_ID
```

## 📄 License

spotify-dl is licensed under the MIT license. See [LICENSE](LICENSE).
