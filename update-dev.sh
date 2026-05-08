#!/bin/bash
cd /opt/bingo-dev

BACKUP_DIR="/opt/bingo-dev/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_FILE="$BACKUP_DIR/bingo_dev_$(date +%Y%m%d_%H%M%S).db"
if [ -f "bingo-dev.db" ]; then
  cp bingo-dev.db "$BACKUP_FILE"
  echo "💾 Backup dev criado: $BACKUP_FILE"
fi
ls -t "$BACKUP_DIR"/bingo_dev_*.db 2>/dev/null | tail -n +8 | xargs -r rm

echo "🔄 Atualizando código (branch develop)..."
git fetch origin
git reset --hard origin/develop

sed -i 's/port=60080/port=60081/' /opt/bingo-dev/app.py
sed -i 's|DB_PATH\s*=\s*os.path.join(APP_DIR, "bingo.db")|DB_PATH       = os.path.join(APP_DIR, "bingo-dev.db")|' /opt/bingo-dev/app.py

echo "🔁 Reiniciando bingo-dev..."
systemctl restart bingo-dev
echo "✅ Ambiente de testes atualizado!"
systemctl status bingo-dev --no-pager | head -5
