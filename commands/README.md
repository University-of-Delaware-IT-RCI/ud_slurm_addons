# Local Slurm Commands

Slurm-centric commands added by IT-HPC staff.

## ssheet_driver

The `ssheet_driver` presents several Slurm commands' output as an interactive spreadsheet using the curses display library:

| Slurm command | Spreadsheet wrapper |
|---------------|---------------------|
| sacct         | ssacct              |
| sinfo         | ssinfo              |
| squeue        | ssqueue             |
| sshare        | ssshare             |
| sstat         | ssstat              |

That's right, we just prefixed the Slurm command with an extra "s" character.  Any flags and arguments to the Slurm command can be passed to the Spreadsheet wrapper command (`--format`, `--sort`, `--account`, etc.).

Each column in the spreadsheet is sized to match the longest string present, rather than truncating at a fixed width as the Slurm commands do.  The display fits the current terminal dimensions but scrolls across the full spreadsheet when the user presses arrow keys or a number of special keys.  The special keys (summarized at the bottom of the on-screen display) are:

- **Q**/**q**:  exit the spreadsheet
- **P**/**p**:  page-up (scroll vertically)
- **N**/**n**:  page-down (scroll vertically)
- **L**/**l**:  page-left (scroll horizontally)
- **R**/**r**:  page-right (scroll horizontally)
- **B**/**b**:  scroll vertically to the top (beginning) of the spreadsheet
- **E**/**e**:  scroll vertically to the end of the spreadsheet

Each of the Spreadsheet wrappers is a symlink to the `ssheet_driver` Python script; the Python script looks at the command the user types to infer which native Slurm command is being wrapped.  The Python code should work with Python 2.6 or better (including Python 3) and uses whatever version the `python` command equates to in your shell/OS.  The Python *curses* library must be present (c'mon, the native Pythong 2.6 on CentOS 6 has it, so it's bound to be there). 