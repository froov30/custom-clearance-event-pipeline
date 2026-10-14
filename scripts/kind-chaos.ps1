<#
Chaos check on kind (D-063, D-067 to D-069). Synthetic data.

    .\scripts\kind-chaos.ps1 [-Seed 7] [-Shipments 6000] [-StepDelay 4]

Needs the cluster from deploy/k8s/README.md steps 2-4 and an empty `ccp` database. The processor is scaled up
here and left at 3. The hold threshold is the ConfigMap's HOLD_ALERT_SECONDS, the same value the demo uses.

Phase A, while the load is in flight: scale the processor 1 -> 3, SIGKILL a validator, inject 5 byte-identical
re-sends of this run's messages (what a validator re-send looks like; the demo must count them as re-emits, D-067),
delete a processor (SIGTERM), restart the Kafka broker and then MongoDB (both mid-backlog), SIGKILL a processor
from the node, delete the monitor, scale 3 -> 2 -> 3.
Phase B, after the backlog drains: freeze a processor with SIGSTOP so only the liveness probe can recover it.

After each phase the run is checked in the api pod: the demo's totals against the simulator's manifest (with the
validator re-emits added to the expected duplicates), and every shipment against a fresh fold of its events.
Exit 0 only if every check passes.
#>
param([int]$Seed = 7, [int]$Shipments = 6000, [double]$StepDelay = 4)

$sim = @("--seed", "$Seed", "--shipments", "$Shipments", "--step-delay", "$StepDelay")
$failed = 0

function Say($msg) { Write-Output ("{0:HH:mm:ss}Z  {1}" -f (Get-Date).ToUniversalTime(), $msg) }

function Live($app) {
    # Running pods not already being deleted.
    $pods = (kubectl -n ccp get pods -l "app=$app" -o json | ConvertFrom-Json).items
    @($pods | Where-Object { $_.status.phase -eq "Running" -and -not $_.metadata.deletionTimestamp } |
        ForEach-Object { $_.metadata.name })
}

function Signal($pod, $sig) {
    # Sent from the node: inside its own PID namespace the app is PID 1, which ignores SIGKILL and SIGSTOP.
    $cid = docker exec ccp-control-plane crictl ps -q --label "io.kubernetes.pod.name=$pod"
    $nodePid = docker exec ccp-control-plane crictl inspect -o go-template --template "{{.info.pid}}" $cid
    docker exec ccp-control-plane kill "-$sig" $nodePid
    Say "kill -$sig $pod (node pid $nodePid)"
}

function Remove($pod) { kubectl -n ccp delete pod $pod --wait=false | Out-Null; Say "deleted pod $pod (SIGTERM)" }

function Restart($pod) {
    kubectl -n ccp delete pod $pod | Out-Null
    Start-Sleep 5
    kubectl -n ccp wait --for=condition=ready pod/$pod --timeout=180s | Out-Null
    Say "$pod restarted and ready"
}

function Scale($n) { kubectl -n ccp scale deploy/processor --replicas=$n | Out-Null; Say "processor scaled to $n" }

function Reemit($count) {
    # Copies of this run's messages, sent again byte for byte, as a validator re-send looks on the wire (D-067).
    $out = Get-Content scripts/chaos_probe.py -Raw | kubectl -n ccp exec -i deploy/api -- python - reemit $count $startJson
    Say "injected re-sends: $out"
}

function Check($label) {
    Say "check after $label ..."
    kubectl -n ccp exec deploy/api -- python -m ccp.demo --verify-only --api-url http://127.0.0.1:8000 `
        --timeout 900 --validated-baseline $baseline @sim | Select-Object -Last 23
    if ($LASTEXITCODE -ne 0) { $script:failed++ }
    kubectl -n ccp exec deploy/api -- python -m ccp.processor.audit
    if ($LASTEXITCODE -ne 0) { $script:failed++ }
}

$empty = kubectl -n ccp exec deploy/api -- python -m ccp.processor.audit
if ($empty -notmatch '"shipments": 0,') { Write-Output "needs an empty ccp database (a fresh cluster): $empty"; exit 2 }

# Messages already on `validated` (other runs on the same topic) are excluded from this run's count.
$startJson = (Get-Content scripts/chaos_probe.py -Raw | kubectl -n ccp exec -i deploy/api -- python - offsets).Trim()
$offsets = ConvertFrom-Json $startJson  # not piped: a piped array arrives as one object
$baseline = [int](($offsets | Measure-Object -Sum).Sum)
Say "validated baseline: $baseline (partition ends $startJson)"

# Phase A: under load
kubectl -n ccp delete job simulator-load --ignore-not-found | Out-Null
$job = (Get-Content deploy/k8s/simulator-job.yaml -Raw) -replace "name: simulator", "name: simulator-load" `
    -replace '"--seed", "42"', ('"' + ($sim -join '", "') + '"')
$job | kubectl apply -f - | Out-Null
Say "load run started: $($sim -join ' ')"
Start-Sleep 8; Scale 3
Start-Sleep 6; Signal @(Live validator)[0] 9
Start-Sleep 4; Reemit 5
Start-Sleep 6; Remove @(Live processor)[0]
Start-Sleep 6; Restart kafka-0
Start-Sleep 6; Signal @(Live processor)[-1] 9
Start-Sleep 4; Restart mongo-0
Start-Sleep 6; Remove @(Live monitor)[0]
Start-Sleep 6; Scale 2
Start-Sleep 10; Scale 3
kubectl -n ccp wait --for=condition=complete job/simulator-load --timeout=600s | Out-Null
Say ("simulator: " + (kubectl -n ccp logs job/simulator-load --tail=1))
Check "phase A (load, validator kill, broker and MongoDB restarts, scaling)"

# Phase B: a hung consumer that only the liveness probe can recover
$frozen = @(Live processor)[0]
Signal $frozen STOP
$t0 = Get-Date
do {
    Start-Sleep 5
    $rc = [int](kubectl -n ccp get pod $frozen -o jsonpath="{.status.containerStatuses[0].restartCount}")
} until ($rc -gt 0 -or ((Get-Date) - $t0).TotalSeconds -gt 360)
Say ("$frozen restartCount=$rc, {0:N0} s after SIGSTOP" -f ((Get-Date) - $t0).TotalSeconds)
if ($rc -eq 0) { $failed++ }
kubectl -n ccp get events --field-selector "involvedObject.name=$frozen" --sort-by=.lastTimestamp `
    -o custom-columns=REASON:.reason,COUNT:.count,MESSAGE:.message | Select-String "Unhealthy|Killing|Started"
Check "phase B (liveness restart)"

kubectl -n ccp get pods
Say "failed checks: $failed"
exit [int]($failed -gt 0)
