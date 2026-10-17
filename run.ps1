param(
    [ValidateSet('all','setup','bench','build','spark','inspect')]
    [string]$Step = 'all',
    [string]$Python = 'python',
    [switch]$Overwrite
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Run-Command {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Ошибка: $Program $Arguments" }
}

if ($Step -in @('all','setup')) {
    Run-Command docker @('compose','up','-d','--wait','--wait-timeout','180')
    Run-Command $Python @('-X','utf8','jobs/run.py','setup')
}
if ($Step -in @('all','bench')) {
    $options = @('-X','utf8','jobs/run.py','bench')
    if ($Overwrite) { $options += '--overwrite' }
    Run-Command $Python $options
}
if ($Step -in @('all','build')) {
    Run-Command $Python @('-X','utf8','jobs/run.py','build')
}
if ($Step -in @('all','spark')) {
    Run-Command $Python @('-X','utf8','jobs/dependencies.py')
    & docker compose --profile tools run --rm spark *> results/spark_console.txt
    $sparkExit = $LASTEXITCODE
    Get-Content results/spark_console.txt
    if ($sparkExit -ne 0) { throw 'Ошибка Spark, см. results/spark_console.txt' }
}
if ($Step -in @('all','inspect')) {
    Run-Command $Python @('-X','utf8','jobs/run.py','inspect')
}
