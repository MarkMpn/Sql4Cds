$version = $args[0]
Write-Host "Set version: $version"

$FullPath = Resolve-Path $PSScriptRoot\..\MarkMpn.Sql4Cds.SSMS.20\source.extension.vsixmanifest
Write-Host $FullPath
[xml]$content = Get-Content $FullPath
$content.PackageManifest.Metadata.Identity.Version = $version
$content.Save($FullPath)

$FullPath = Resolve-Path $PSScriptRoot\..\MarkMpn.Sql4Cds.SSMS.21\source.extension.vsixmanifest
Write-Host $FullPath
[xml]$content = Get-Content $FullPath
$content.PackageManifest.Metadata.Identity.Version = $version
$content.Save($FullPath)

$VersionPath = "$PSScriptRoot\sql4cds-version.txt"
Write-Host $VersionPath
Out-File -FilePath $VersionPath -InputObject $version