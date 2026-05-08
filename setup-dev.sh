#!/bin/bash
set -e

echo "======================================"
echo "  Setup Ambiente de Testes — Bingo"
echo "======================================"

# 1. Copia o projeto
echo ""
echo "📁 [1/6] Copiando projeto para /opt/bingo-dev..."
rm -rf /opt/bingo-dev
cp -r /opt/bingo /opt/bingo-dev
echo "✅ Projeto copiado"

# 2. Copia o banco de produção com nome diferente
echo ""
echo "🗄  [2/6] Copiando banco de produção como bingo-dev.db..."
cp /opt/bingo/bingo.db /opt/bingo-dev/bingo-dev.db
echo "✅ Banco copiado como bingo-dev.db"

# 3. Ajusta porta e nome do banco no app de testes
echo ""
echo "⚙️  [3/6] Ajustando porta para 60081 e banco para bingo-dev.db..."
sed -i 's/port=60080/port=60081/' /opt/bingo-dev/app.py
sed -i 's|DB_PATH = os.path.join(APP_DIR, "bingo.db")|DB_PATH = os.path.join(APP_DIR, "bingo-dev.db")|' /opt/bingo-dev/app.py
grep "app.run" /opt/bingo-dev/app.py | tail -1
grep "DB_PATH" /opt/bingo-dev/app.py | head -1
echo "✅ Porta e banco ajustados"

# 4. Cria o serviço systemd
echo ""
echo "🔧 [4/6] Criando serviço bingo-dev..."
cat > /etc/systemd/system/bingo-dev.service << 'SERVICE'
[Unit]
Description=Bingo DEV — Ambiente de Testes
After=network.target

[Service]
WorkingDirectory=/opt/bingo-dev
ExecStart=/opt/bingo-dev/venv/bin/python3 /opt/bingo-dev/app.py
Restart=always
RestartSec=5
User=root
Environment=PYTHONUNBUFFERED=1
TimeoutStopSec=30
KillSignal=SIGTERM
KillMode=mixed

[Install]
WantedBy=multi-user.target
SERVICE
systemctl daemon-reload
systemctl enable bingo-dev
systemctl start bingo-dev
echo "✅ Serviço bingo-dev criado e iniciado"

# 5. Configura Nginx
echo ""
echo "🌐 [5/6] Configurando Nginx para dev.bingocoracaodemaria.cloud..."
cat > /etc/nginx/sites-available/bingo-dev << 'NGINX'
server {
    listen 80;
    server_name dev.bingocoracaodemaria.cloud;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name dev.bingocoracaodemaria.cloud;

    ssl_certificate     /etc/letsencrypt/live/bingocoracaodemaria.cloud/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/bingocoracaodemaria.cloud/privkey.pem;
    include             /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam         /etc/letsencrypt/ssl-dhparams.pem;

    location / {
        proxy_pass         http://127.0.0.1:60081;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }
}
NGINX
ln -sf /etc/nginx/sites-available/bingo-dev /etc/nginx/sites-enabled/bingo-dev
nginx -t && systemctl reload nginx
echo "✅ Nginx configurado"

# 6. Cria script de update para dev
echo ""
echo "🔄 [6/6] Criando bingo-dev-update..."
cat > /opt/bingo-dev/update-dev.sh << 'UPDATESCRIPT'
#!/bin/bash
cd /opt/bingo-dev

# Backup do banco de dev
BACKUP_DIR="/opt/bingo-dev/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_FILE="$BACKUP_DIR/bingo_dev_$(date +%Y%m%d_%H%M%S).db"
if [ -f "bingo-dev.db" ]; then
  cp bingo-dev.db "$BACKUP_FILE"
  echo "💾 Backup dev criado: $BACKUP_FILE"
fi
ls -t "$BACKUP_DIR"/bingo_dev_*.db 2>/dev/null | tail -n +8 | xargs -r rm

# Puxa do GitHub
echo "🔄 Atualizando código..."
git pull origin main

# Ajusta porta e banco para dev
sed -i 's/port=60080/port=60081/' /opt/bingo-dev/app.py
sed -i 's|DB_PATH = os.path.join(APP_DIR, "bingo.db")|DB_PATH = os.path.join(APP_DIR, "bingo-dev.db")|' /opt/bingo-dev/app.py

# Reinicia
echo "🔁 Reiniciando bingo-dev..."
systemctl restart bingo-dev
echo "✅ Ambiente de testes atualizado!"
systemctl status bingo-dev --no-pager
UPDATESCRIPT
chmod +x /opt/bingo-dev/update-dev.sh
ln -sf /opt/bingo-dev/update-dev.sh /usr/local/bin/bingo-dev-update
echo "✅ bingo-dev-update disponível globalmente"

echo ""
echo "======================================"
echo "  ✅ Ambiente de testes pronto!"
echo "======================================"
echo ""
echo "  🌐 URL:     https://dev.bingocoracaodemaria.cloud"
echo "  📁 Pasta:   /opt/bingo-dev"
echo "  ⚙️  Serviço: bingo-dev"
echo "  🔄 Update:  bingo-dev-update"
echo ""
echo "  ⚠️  Lembre de apontar o DNS:"
echo "  dev.bingocoracaodemaria.cloud → $(curl -s ifconfig.me)"
echo ""
