<#
.SYNOPSIS
    Bump every ThunderPropagator-family PackageVersion in a repo's Directory.Packages.props
    to the latest version published on nuget.org -- prerelease/beta included.

.DESCRIPTION
    Lives in ThunderPropagator.SharedBuild and is downloaded into every consuming repo's
    .shared-props\ folder by the same DownloadSharedProps target that fetches
    Shared.Build.props / Shared.Nuget.props / Shared.PackageIds.props / Shared.DependencyUpdater.props.
    A consuming repo may additionally keep a committed copy of this script at
    .github\scripts\ (so the check has no download/network dependency for the script
    itself -- Shared.DependencyUpdater.props prefers that copy when present). Path
    resolution below works unmodified from either location, or any other folder inside
    the repo, since it walks up to find Directory.Packages.props as the repo-root marker
    instead of assuming a fixed relative layout.

    Fully auto-discovering: it reads Shared.PackageIds.props to learn every
    "{Name}PackageId" pattern ThunderPropagator publishes, then scans the target repo's
    own Directory.Packages.props for PackageVersion entries whose Include is one of those
    PackageId properties. Entries that share the same version property (e.g.
    BuildingBlocksPackageId and BuildingBlocksModulesPackageId both pinned via
    BuildingBlocksVersion) are resolved and updated together; entries pinned with a
    literal version string are updated in place individually. Any repo that adopts the
    "$(XxxPackageId)" convention works with this script unmodified -- nothing here is
    specific to any one consuming repo.

    nuget.org is a public feed with no auth required, so no token or source registration
    is needed -- this queries "$Source" directly regardless of what's in NuGet.Config.

.PARAMETER PropsPath
    Path to the target repo's Directory.Packages.props. Defaults to the nearest
    Directory.Packages.props found by walking up from this script's own folder -- works
    whether this script lives in <repo>\.shared-props\, <repo>\.github\scripts\, or
    anywhere else inside the repo.

.PARAMETER SharedPackageIdsPath
    Path to Shared.PackageIds.props. Defaults to the copy sitting next to this script if
    present, otherwise <repo root>\.shared-props\Shared.PackageIds.props.

.PARAMETER Source
    NuGet v3 service index to search. Defaults to nuget.org.

.PARAMETER Check
    Print current vs. latest version for every discovered dependency and exit without writing.

.PARAMETER PackageId
    Restrict to a single package id instead of every ThunderPropagator-family entry.

    Combined with -VersionOnly: just print that package's latest published version and exit --
    no Directory.Packages.props / Shared.PackageIds.props discovery at all.

    Without -VersionOnly: run the normal discovery/compare/apply flow (respects -Check and
    -WhatIf same as an unscoped run), but narrowed to just this package id's entry (or entries,
    if it shares a version property with a sibling -- see -VersionOnly's note on that). Still
    figures out on its own whether that entry is pinned via a shared version PROPERTY ("key",
    e.g. Version="$(BuildingBlocksVersion)") or a literal version string, and updates whichever
    one actually holds the version -- same detection Directory.Packages.props-wide runs already
    use, just scoped to one package.

.PARAMETER VersionOnly
    Print the latest published version of -PackageId and exit -- nothing else runs: no
    Directory.Packages.props / Shared.PackageIds.props discovery, no comparison, no write.
    Requires -PackageId. Takes priority over -Check if both are somehow passed.

.EXAMPLE
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1 -Check
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1 -WhatIf
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1 -PropsPath ..\Directory.Packages.props
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1 -PackageId ThunderPropagator.BuildingBlocks -VersionOnly
    pwsh .shared-props/Update-ThunderPropagatorDependencies.ps1 -PackageId ThunderPropagator.BuildingBlocks
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [string] $PropsPath            = "",
    [string] $SharedPackageIdsPath = "",
    [string] $Source               = "https://api.nuget.org/v3/index.json",
    [switch] $Check,
    [string] $PackageId            = "",
    [switch] $VersionOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference    = 'SilentlyContinue'

# ── Helpers (defined up front: -VersionOnly below needs Get-LatestPackageVersion
#    before anything else in the script runs) ───────────────────────────────────

function Write-Step { param($m) Write-Host "  -> $m" -ForegroundColor Cyan }
function Write-Ok   { param($m) Write-Host "  OK $m" -ForegroundColor Green }
function Write-Warn { param($m) Write-Host "  !! $m" -ForegroundColor Yellow }

function Get-VersionSortKey {
    # Deliberately NOT [System.Management.Automation.SemanticVersion]: observed in practice to
    # rank "1.0.1-beta.79" ABOVE "1.0.1-beta.119" -- its prerelease-label comparison appears to
    # compare the label as plain text ('7' > '1' as the first differing character), not as a
    # numeric SemVer identifier (119 > 79). Building a zero-padded string key instead, so an
    # ordinary lexicographic/ordinal comparison is numeric-correct without depending on that
    # class's behavior at all: every dot-separated segment that is ALL DIGITS (in both the core
    # X.Y.Z and any prerelease identifiers) gets left-padded to a fixed width, so "79" (->
    # "0000000079") sorts below "119" (-> "0000000119") the way plain strings compare. A version
    # with no prerelease outranks one that has any prerelease of the same core version (release >
    # prerelease, per SemVer precedence rules) -- encoded with a trailing "~" (0x7E, sorts after
    # the "-" that starts every prerelease key).
    param([string]$Version)

    function Get-PaddedSegment {
        param([string]$Segment)
        if ($Segment -match '^\d+$') { return $Segment.PadLeft(10, '0') }
        return $Segment
    }

    $core = $Version
    $plusIndex = $core.IndexOf('+')
    if ($plusIndex -ge 0) { $core = $core.Substring(0, $plusIndex) }   # build metadata plays no part in precedence

    $pre = $null
    $dashIndex = $core.IndexOf('-')
    if ($dashIndex -ge 0) {
        $pre  = $core.Substring($dashIndex + 1)
        $core = $core.Substring(0, $dashIndex)
    }

    $coreKey = ($core -split '\.' | ForEach-Object { Get-PaddedSegment $_ }) -join '.'
    if ($null -eq $pre) { return "$coreKey~" }

    $preKey = ($pre -split '\.' | ForEach-Object { Get-PaddedSegment $_ }) -join '.'
    return "$coreKey-$preKey"
}

$script:FlatContainerBaseUrlCache = @{}

function Get-FlatContainerBaseUrl {
    # Resolves the "PackageBaseAddress/3.0.0" resource from a v3 service index. This is the
    # NuGet flat-container API: GET {base}/{id-lower}/index.json returns { "versions": [...] },
    # the complete list of every version ever published for that package -- no paging, no
    # relevance ranking, no result-count cap. Cached per source since every package lookup
    # against the same $Source shares one service index.
    param([string]$SourceUrl)

    if ($script:FlatContainerBaseUrlCache.ContainsKey($SourceUrl)) {
        return $script:FlatContainerBaseUrlCache[$SourceUrl]
    }

    $index    = Invoke-RestMethod -Uri $SourceUrl -ErrorAction Stop
    $resource = $index.resources | Where-Object { $_.'@type' -like 'PackageBaseAddress/*' } | Select-Object -First 1
    if (-not $resource) {
        throw "No 'PackageBaseAddress' resource found in service index '$SourceUrl' -- is this a valid v3 NuGet feed?"
    }

    $base = $resource.'@id'.TrimEnd('/')
    $script:FlatContainerBaseUrlCache[$SourceUrl] = $base
    return $base
}

function Get-LatestPackageVersion {
    # Deliberately NOT "dotnet package search": that queries a relevance-ranked search index
    # which pages its results (a default page size, not every published version) and doesn't
    # guarantee version-descending order within that page. For a package with many prerelease
    # versions, the true latest can fall outside the returned page -- observed in practice as
    # this function returning an OLDER version than what's already pinned (a regression), and
    # returning a DIFFERENT wrong answer across repeated calls. The flat-container endpoint
    # below returns the complete, deterministic version list instead, so sorting it is reliable.
    #
    # Trade-off: flat-container includes unlisted versions (search excludes them by default).
    # Acceptable here since this only ever targets this repo's own family packages.
    param([string]$PackageId, [string]$SourceUrl)

    try {
        $base       = Get-FlatContainerBaseUrl -SourceUrl $SourceUrl
        $idLower    = $PackageId.ToLowerInvariant()
        $versionDoc = Invoke-RestMethod -Uri "$base/$idLower/index.json" -ErrorAction Stop
    } catch {
        Write-Warn "  '$PackageId': version list lookup failed against '$SourceUrl' ($($_.Exception.Message)) -- skipping"
        return $null
    }

    $versions = $versionDoc.versions
    if (-not $versions) {
        Write-Warn "  '$PackageId': no published versions found on '$SourceUrl' -- skipping"
        return $null
    }

    return $versions | Sort-Object { Get-VersionSortKey $_ } -Descending | Select-Object -First 1
}

# ── -VersionOnly: resolve one package's latest version and stop right here ─────
#    Bypasses Directory.Packages.props / Shared.PackageIds.props entirely -- neither
#    is needed just to answer "what's the latest version of <id>", and requiring them
#    would make this mode fail outside a fully-restored repo for no reason. Never
#    writes anything, regardless of -Check.
if ($VersionOnly) {
    if ([string]::IsNullOrWhiteSpace($PackageId)) {
        throw "-VersionOnly requires -PackageId <id>."
    }
    $latest = Get-LatestPackageVersion -PackageId $PackageId -SourceUrl $Source
    if (-not $latest) {
        throw "Could not resolve a version for '$PackageId' on '$Source'."
    }
    Write-Output $latest
    exit 0
}

# ── Resolve paths (works from .shared-props\, .github\scripts\, or any other
#    folder inside the target repo -- walks up to find Directory.Packages.props as
#    the repo-root marker instead of assuming a fixed relative layout) ───────────

function Find-RepoRoot {
    param([string]$StartDirectory)
    $dir = $StartDirectory
    while ($dir) {
        if (Test-Path (Join-Path $dir "Directory.Packages.props")) { return $dir }
        $parent = Split-Path $dir -Parent
        if (-not $parent -or $parent -eq $dir) { return $null }
        $dir = $parent
    }
    return $null
}

if ([string]::IsNullOrWhiteSpace($PropsPath)) {
    $repoRoot = Find-RepoRoot -StartDirectory $PSScriptRoot
    if (-not $repoRoot) {
        throw "Could not locate Directory.Packages.props by walking up from '$PSScriptRoot'. Pass -PropsPath explicitly."
    }
    $PropsPath = Join-Path $repoRoot "Directory.Packages.props"
}
if (-not (Test-Path $PropsPath)) {
    throw "Directory.Packages.props not found at '$PropsPath'. Pass -PropsPath explicitly."
}

if ([string]::IsNullOrWhiteSpace($SharedPackageIdsPath)) {
    # Prefer a copy sitting right next to this script (the .shared-props\ case);
    # otherwise fall back to <repo root>\.shared-props\Shared.PackageIds.props
    # (the repo-local-copy case, e.g. .github\scripts\).
    $sibling              = Join-Path $PSScriptRoot "Shared.PackageIds.props"
    $SharedPackageIdsPath = if (Test-Path $sibling) { $sibling } else { Join-Path (Split-Path $PropsPath -Parent) ".shared-props/Shared.PackageIds.props" }
}
if (-not (Test-Path $SharedPackageIdsPath)) {
    throw "Shared.PackageIds.props not found at '$SharedPackageIdsPath'. Run 'dotnet restore' first, or pass -SharedPackageIdsPath."
}

# ── Step 1: learn every "{Name}PackageId" pattern from Shared.PackageIds.props ─
#    e.g. BuildingBlocksPackageId -> "ThunderPropagator.BuildingBlocks"
#    (literal text captured up to the first "$(...)" suffix token)

$sharedPackageIdsContent = Get-Content -Path $SharedPackageIdsPath -Raw
$packageIdMap            = @{}

foreach ($m in [regex]::Matches($sharedPackageIdsContent, '<(?<prop>\w+PackageId)>(?<lit>[^$<]+)')) {
    $packageIdMap[$m.Groups['prop'].Value] = $m.Groups['lit'].Value.Trim()
}

if ($packageIdMap.Count -eq 0) {
    throw "No '{Name}PackageId' properties found in '$SharedPackageIdsPath' -- nothing to discover."
}

# ── Step 2: scan the target repo's Directory.Packages.props for PackageVersion
#    entries whose Include references one of those PackageId properties ───────
#    Groups entries that share a version PROPERTY together (so BuildingBlocks +
#    BuildingBlocksModules resolve and update as one family); entries pinned
#    with a literal version string are tracked individually.

$propsContent = Get-Content -Path $PropsPath -Raw
$pattern      = '<PackageVersion\s+Include="\$\((?<propid>\w+PackageId)\)"\s+Version="(?:\$\((?<verprop>\w+)\)|(?<verlit>[^"$][^"]*))"'

$byVersionProperty = [ordered]@{}   # verprop -> list of literal package ids
$byLiteralEntry    = @()            # one entry per literally-pinned PackageVersion line

foreach ($m in [regex]::Matches($propsContent, $pattern)) {
    $propId = $m.Groups['propid'].Value
    if (-not $packageIdMap.ContainsKey($propId)) { continue }   # not a known ThunderPropagator package id
    $literalId = $packageIdMap[$propId]

    if ($m.Groups['verprop'].Success) {
        $verProp = $m.Groups['verprop'].Value
        if (-not $byVersionProperty.Contains($verProp)) { $byVersionProperty[$verProp] = @() }
        $byVersionProperty[$verProp] += $literalId
    } else {
        $byLiteralEntry += [pscustomobject]@{
            PropId     = $propId
            LiteralId  = $literalId
            Current    = $m.Groups['verlit'].Value
        }
    }
}

if ($byVersionProperty.Count -eq 0 -and $byLiteralEntry.Count -eq 0) {
    Write-Warn "No ThunderPropagator-family PackageVersion entries found in '$PropsPath' -- nothing to update."
    exit 0
}

# ── Optional: narrow everything down to a single -PackageId ───────────────────
#    (only reached when -VersionOnly wasn't set -- that mode already exited above).
#    Figures out where THIS package's version actually lives -- a shared property
#    ("key", $byVersionProperty) or a literal string on its own PackageVersion line
#    ($byLiteralEntry) -- and drops everything else, so the resolve/compare/apply
#    steps below run unmodified but scoped to just this one package (and any
#    sibling that happens to share the same version property with it).
if (-not [string]::IsNullOrWhiteSpace($PackageId)) {
    $scopedByVersionProperty = [ordered]@{}
    foreach ($verProp in $byVersionProperty.Keys) {
        if ($byVersionProperty[$verProp] -contains $PackageId) {
            $scopedByVersionProperty[$verProp] = $byVersionProperty[$verProp]
        }
    }
    $byVersionProperty = $scopedByVersionProperty
    $byLiteralEntry     = @($byLiteralEntry | Where-Object { $_.LiteralId -ieq $PackageId })

    if ($byVersionProperty.Count -eq 0 -and $byLiteralEntry.Count -eq 0) {
        Write-Warn "'$PackageId' is not a ThunderPropagator-family PackageVersion entry in '$PropsPath' -- nothing to do."
        exit 0
    }
}

# ── Banner ────────────────────────────────────────────────────────────────────
# nuget.org is public and needs no auth or source registration -- $Source is
# queried directly via "dotnet package search --source", independent of
# whatever sources are configured in this repo's own NuGet.Config.

Write-Host ""
Write-Host "ThunderPropagator Dependency Updater" -ForegroundColor White
Write-Host "  Props      : $PropsPath"
Write-Host "  PackageIds : $SharedPackageIdsPath"
Write-Host "  Source     : $Source"
Write-Host "  Mode       : $(if ($Check) { 'check only' } else { 'update' })"
Write-Host ""

# ── Resolve latest version per discovered dependency ──────────────────────────

$propertyUpdates = @()   # @{ Property; Old; New }
$literalUpdates  = @()   # @{ PropId; LiteralId; Old; New }

foreach ($verProp in $byVersionProperty.Keys) {
    $literalIds = $byVersionProperty[$verProp] | Select-Object -Unique
    Write-Step "Resolving '$verProp' from: $($literalIds -join ', ')"

    $candidates = foreach ($id in $literalIds) {
        $v = Get-LatestPackageVersion -PackageId $id -SourceUrl $Source
        if ($v) { [pscustomobject]@{ PackageId = $id; Version = $v } }
    }

    if (-not $candidates) {
        Write-Warn "  No versions resolved for '$verProp' -- leaving as-is."
        continue
    }

    # @(...) forces array context: when only one candidate resolves, PowerShell would
    # otherwise collapse this to a bare scalar string, which has no .Count property
    # under Set-StrictMode.
    $distinctVersions = @($candidates.Version | Select-Object -Unique)
    if ($distinctVersions.Count -gt 1) {
        Write-Warn "  '$verProp' package ids disagree on latest version: $($distinctVersions -join ', ') -- using the highest."
    }

    $latest = $candidates |
              Sort-Object { Get-VersionSortKey $_.Version } -Descending |
              Select-Object -First 1 -ExpandProperty Version

    $currentMatch = [regex]::Match($propsContent, "<$verProp>([^<]*)</$verProp>")
    $current      = if ($currentMatch.Success) { $currentMatch.Groups[1].Value } else { $null }

    if (-not $current) {
        Write-Warn "  Property '$verProp' not found as its own element in $PropsPath -- skipping."
        continue
    }

    if ($current -eq $latest) {
        Write-Ok "  $verProp is already latest: $current"
    } else {
        Write-Host "  $verProp : $current --> $latest" -ForegroundColor Yellow
        $propertyUpdates += [pscustomobject]@{ Property = $verProp; Old = $current; New = $latest }
    }
}

foreach ($entry in $byLiteralEntry) {
    Write-Step "Resolving literal-pinned '$($entry.LiteralId)'"
    $latest = Get-LatestPackageVersion -PackageId $entry.LiteralId -SourceUrl $Source
    if (-not $latest) { continue }

    if ($entry.Current -eq $latest) {
        Write-Ok "  $($entry.LiteralId) is already latest: $($entry.Current)"
    } else {
        Write-Host "  $($entry.LiteralId) : $($entry.Current) --> $latest" -ForegroundColor Yellow
        $literalUpdates += [pscustomobject]@{ PropId = $entry.PropId; LiteralId = $entry.LiteralId; Old = $entry.Current; New = $latest }
    }
}

$totalUpdates = $propertyUpdates.Count + $literalUpdates.Count

# ── Check mode: report only ───────────────────────────────────────────────────

Write-Host ""
if ($Check) {
    if ($totalUpdates -eq 0) {
        Write-Ok "All ThunderPropagator dependencies are already at the latest version."
    } else {
        Write-Warn "$totalUpdates update(s) available (run without -Check to apply)."
    }
    exit 0
}

if ($totalUpdates -eq 0) {
    Write-Ok "Nothing to update. $PropsPath is untouched."
    exit 0
}

# ── Apply updates ─────────────────────────────────────────────────────────────

foreach ($u in $propertyUpdates) {
    $propsContent = $propsContent -replace "<$($u.Property)>[^<]*</$($u.Property)>", "<$($u.Property)>$($u.New)</$($u.Property)>"
}

foreach ($u in $literalUpdates) {
    $linePattern = "(<PackageVersion\s+Include=`"\`$\($($u.PropId)\)`"\s+Version=`")[^`"]*(`")"
    # Single-quoted replacement so .NET regex -- not PowerShell -- interprets ${1}/${2} as backreferences.
    $replacement = '${1}' + $u.New + '${2}'
    $propsContent = $propsContent -replace $linePattern, $replacement
}

if ($PSCmdlet.ShouldProcess($PropsPath, "Write $totalUpdates updated version(s)")) {
    Set-Content -Path $PropsPath -Value $propsContent -NoNewline -Encoding UTF8
    Write-Ok "Wrote $totalUpdates update(s) to $PropsPath"
}

Write-Host ""
Write-Host "----------------------------------------------------" -ForegroundColor White
foreach ($u in $propertyUpdates) { Write-Host "  $($u.Property): $($u.Old) -> $($u.New)" -ForegroundColor Green }
foreach ($u in $literalUpdates)  { Write-Host "  $($u.LiteralId): $($u.Old) -> $($u.New)" -ForegroundColor Green }
Write-Host "----------------------------------------------------" -ForegroundColor White
Write-Host "  Run 'dotnet restore' to pull the updated package(s)." -ForegroundColor DarkGray
