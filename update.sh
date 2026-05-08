#!/bin/bash
echo "🔄 Atualizando Bingo..."
cd /opt/bingo
git pull origin main
echo "🔁 Reiniciando serviço..."
systemctl restart bingo
echo "✅ Atualização concluída!"
systemctl status bingo --no-pager
