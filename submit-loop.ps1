# submit-loop.ps1
# Repeatedly submits the SparkPi SparkApplication. The previous application is
# deleted only after it has reached a terminal state; otherwise the round is
# skipped so a running job is never killed mid-flight.

$Manifest = "setup/spark-pi.yaml"
$AppName = "spark-pi-yunikorn"
$TerminalStates = @('COMPLETED', 'FAILED', 'SUBMISSION_FAILED')
$IntervalSeconds = 60

while ($true) {
    Write-Host "=== $(Get-Date) ==="

    # Query the SparkApplication state. kubectl exits non-zero when the
    # resource does not exist (first run, or already cleaned up).
    $state = kubectl get sparkapplication $AppName -o jsonpath='{.status.applicationState.state}' 2>$null
    $exists = ($LASTEXITCODE -eq 0)

    if (-not $exists) {
        Write-Host "No existing SparkApplication found, submitting a new one."
        kubectl apply -f $Manifest
        Start-Sleep -Seconds $IntervalSeconds
        continue
    }

    $isTerminal = $state -and ($TerminalStates -contains $state.ToString().ToUpper())

    if ($isTerminal) {
        # Terminal state: safe to delete the old app and submit a new one.
        $appID = kubectl get pod "$AppName-driver" -o jsonpath='{.metadata.labels.applicationId}' 2>$null
        if (-not $appID) {
            $appID = kubectl get pod "$AppName-driver" -o jsonpath='{.metadata.labels.spark-app-selector}' 2>$null
        }

        if ($appID) {
            Write-Host "Current application ID (will be deleted): $appID [state=$state]"
            kubectl get pods -l spark-app-selector=$appID -o custom-columns=NAME:.metadata.name,STATUS:.status.phase --no-headers
        } else {
            Write-Host "SparkApplication reached terminal state '$state' but no driver pod was found; deleting anyway."
        }

        kubectl delete -f $Manifest --ignore-not-found
        kubectl apply -f $Manifest
        Write-Host "Submitted new SparkApplication."
    } else {
        $displayState = if ($state) { "$state" } else { "<unknown, status not yet reported>" }
        Write-Host "SparkApplication still in state '$displayState', skip this round (waiting for: $($TerminalStates -join ', '))."
    }

    Start-Sleep -Seconds $IntervalSeconds
}