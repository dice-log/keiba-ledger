# Git 初期化・初回コミット・プッシュ用スクリプト
# 実行: .\scripts\tools\git_setup.ps1
# リモート未設定時は git remote add origin <URL> で追加後、git push -u origin main

Set-Location $PSScriptRoot\..\..
git init
git add .
git status
git commit -m "Phase 1: JV-Link fetch, DB schema, normalize, sync"
Write-Host ""
Write-Host "リモートを追加する場合:"
Write-Host '  git remote add origin https://github.com/USER/keiba-ledger.git'
Write-Host ""
Write-Host "プッシュする場合:"
Write-Host "  git branch -M main"
Write-Host "  git push -u origin main"
