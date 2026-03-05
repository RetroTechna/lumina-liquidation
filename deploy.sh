#!/bin/bash
# Deploy Lumina Liquidation — push to GitHub, Cloudflare Pages auto-deploys
git add -A
echo -n "Commit message: "
read msg
git commit -m "${msg:-Update site}"
git push origin main
echo "Pushed to GitHub. Cloudflare Pages will auto-deploy in ~30 seconds."
