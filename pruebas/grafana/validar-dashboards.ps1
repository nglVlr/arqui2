# Valida que todas las consultas de los dashboards de Grafana funcionen.
# Ejecuta cada consulta de cada panel contra la API de Grafana (/api/ds/query)
# usando el usuario admin del .env, y muestra "ok" con la cantidad de filas o el error.
# Uso (desde la raíz del proyecto, con el sistema levantado):
#   .\pruebas\grafana\validar-dashboards.ps1 -Archivos (Get-ChildItem infra\grafana\dashboards\*.json).FullName
param([string[]]$Archivos)
$raiz = (Resolve-Path "$PSScriptRoot\..\..").Path
$envs = @{}; Get-Content "$raiz\.env" | ? { $_ -match '^[A-Z_]+=' } | % { $k, $v = $_ -split '=', 2; $envs[$k] = $v }
$cred = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("$($envs.GRAFANA_ADMIN_USER):$($envs.GRAFANA_ADMIN_PASSWORD)"))
$h = @{ Authorization = "Basic $cred" }
foreach ($archivo in $Archivos) {
  $d = Get-Content $archivo -Raw -Encoding utf8 | ConvertFrom-Json
  "== $($d.title)"
  foreach ($p in $d.panels) {
    foreach ($t in $p.targets) {
      $q = @{ refId = "A"; datasource = $p.datasource; format = $t.format; rawSql = $t.rawSql; rawQuery = $true }
      if ($t.query -ne $null) { $q = $t | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable; $q.datasource = $p.datasource
        # Las variables del dashboard (ej. ${busqueda:raw}) las reemplaza el navegador, no la API: se usa "*"
        if ($q.query -match '\$\{') { $q.query = '*' } }
      $body = @{ from = "now-1h"; to = "now"; queries = @($q) } | ConvertTo-Json -Depth 20
      try {
        $r = Invoke-RestMethod "http://localhost:3000/api/ds/query" -Method Post -Headers $h -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($body))
        $res = $r.results.A
        if ($res.error) { "  ERROR  [$($p.title)] $($res.error)" }
        else {
          $filas = 0; foreach ($f in $res.frames) { if ($f.data.values.Count -gt 0) { $filas += $f.data.values[0].Count } }
          "  ok     [$($p.title)] filas=$filas"
        }
      } catch { "  ERROR  [$($p.title)] $($_.ErrorDetails.Message)" }
    }
  }
}
