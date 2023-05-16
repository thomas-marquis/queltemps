# Get app version composite action

This action get app version string from given version file.

Version file must contains version in first line.

## Inputs

## `versionFile`

**Optional** The name of the file containing version. Default `"VERSION"`.

## Outputs

## `version`

The app version containing in the file.

## Example usage


```yaml
steps:
  - uses: actions/get-app-version@v1
    id: appVersion
    with:
      versionFile: VERSION.txt
  - uses: ...
    with:
      appVersion: ${{ steps.appVersion.outputs.version }}
```
