# Triage Labels

The skills speak in terms of five canonical triage roles. This repo tracks issues
as local markdown files, so a "label" is the value of the `Status:` line near the
top of the issue file — there is no label API to call.

| Label in mattpocock/skills | `Status:` value in our files | Meaning                                  |
| -------------------------- | ---------------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`               | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`                 | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`            | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`            | Requires human implementation            |
| `wontfix`                  | `wontfix`                    | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), write the
corresponding value from the right-hand column into the file's `Status:` line.

Edit the right-hand column to match whatever vocabulary you actually use.
