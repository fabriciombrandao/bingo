#!/bin/bash
cd /opt/bingo

# 1. Backup do banco antes de qualquer coisa
BACKUP_DIR="/opt/bingo/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_FILE="$BACKUP_DIR/bingo_$(date +%Y%m%d_%H%M%S).db"
if [ -f "bingo.db" ]; then
  cp bingo.db "$BACKUP_FILE"
  echo "💾 Backup criado: $BACKUP_FILE"
else
  echo "⚠️  bingo.db não encontrado — backup ignorado"
fi

# Mantém apenas os últimos 7 backups
ls -t "$BACKUP_DIR"/bingo_*.db 2>/dev/null | tail -n +8 | xargs -r rm
echo "🧹 Backups antigos removidos (mantendo últimos 7)"

# 2. Atualiza código
echo "🔄 Atualizando Bingo..."
git pull origin main

# 3. Reinicia serviço
echo "🔁 Reiniciando serviço..."
systemctl restart bingo
echo "✅ Atualização concluída!"
systemctl status bingo --no-pager
