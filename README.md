# rdrive

Rust application to sync files with Google Drive

## How to configure
By default, this will create an empty configuration file called `config.json`.

### Config location
#### Linux 
`$XDG_CONFIG_HOME/rdrive` or `$HOME/.config/rdrive`

#### Mac
`$HOME/Library/Preferences/rdrive`

#### Windows
`%LOCALAPPDATA%/rdrive`

### Config structure
```json
{
  "exclude": [],
  "include": [],
  "root_dir": ""
}
```

| Property | Description                                                                  | Default Value                                                 |
|----------|------------------------------------------------------------------------------|---------------------------------------------------------------|
| exclude  | Array of ant matchable strings to exclude                                    | `[]`                                                          |
| include  | Array of ant matchable strings to include (if populated, exclude is ignored) | `[]`                                                          |
| root_dir | The directory to sync to                                                     | `$HOME/rdrive` for Linux/Mac<br>`%USERPROFILE%` for Windows|

## Data location
### Linux
`$XDG_DATA_HOME` or `$HOME/.local/share`

### Mac
`~/Library/rdrive`

### Windows
`%LOCALAPPDATA%/rdrive`
