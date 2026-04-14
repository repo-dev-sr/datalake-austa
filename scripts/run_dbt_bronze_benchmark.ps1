$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
& 'C:\Users\Rafael\Desktop\datalake-austa\.venv\Scripts\dbt.exe' run --project-dir dbt --profiles-dir dbt --select bronze_tasy_procedimento_paciente 2>&1 | Tee-Object -FilePath 'C:\Users\Rafael\Desktop\datalake-austa\logs\benchmark_dbt_bronze_v2.txt'
$stopwatch.Stop()
Write-Output ("DBT_ELAPSED_SECONDS=" + $stopwatch.Elapsed.TotalSeconds)
