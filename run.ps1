Set-Location $PSScriptRoot

go build -o bully.exe bully.go
if ($LASTEXITCODE -ne 0) { return }

Start-Process .\bully.exe -ArgumentList "0 live" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "1 live" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "2 live" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "3 live" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "4 live_init" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "5 live" -NoNewWindow
Start-Process .\bully.exe -ArgumentList "6 live" -NoNewWindow
.\bully.exe 7 dead
