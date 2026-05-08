# BINGO WHATSAPP — Sistema de Cobrança
## Contexto do Projeto para IA (Claude)

## 🗂 Estrutura do Projeto

/opt/bingo/
├── app.py                  # Backend Flask principal
├── templates/              # HTMLs (login, index, cadastro, camisetas, telao)
├── templates_msg/          # Templates de mensagem WhatsApp (.json)
├── static/imagens/         # Banners e rodapés dos templates
└── venv/                   # Ambiente virtual Python

## 🚀 Stack

- Backend: Python 3.10 + Flask
- Banco: SQLite (WAL mode)
- WhatsApp: Twilio API
- Frontend: HTML/CSS/JS puro (index.html ~7000 linhas)
- Servidor: VPS Ubuntu — /opt/bingo — porta 60080
- Serviço: bingo.service (systemd)

## 🗄 Tabelas do Banco

- contatos       → lotes/cartelas (núcleo do sistema)
- usuarios       → usuários com perfis e permissões
- config         → configurações chave-valor
- log_envios     → histórico de disparos WhatsApp
- auditoria      → log de todas as ações
- premios        → prêmios do sorteio
- sorteios       → sorteios criados
- sorteio_premios, sorteio_numeros, sorteio_ganhadores
- camisetas_pedidos, camisetas_adicionais, camisetas_pagamentos

## 📋 Campos importantes de contatos

status: Disponivel | Pendente | Pago | Desmembrado
origem_id: NULL=lote normal, preenchido=cartela desmembrada

## 🔑 Regras de Negócio

- Só dispara WhatsApp para status=Pendente
- Previsão de pagamento futura = não dispara
- dias_disparo=0 dispara todos os pendentes
- Nunca dispara 2x no mesmo dia para o mesmo contato
- Desmembramento: lote Disponivel com intervalo "X a Y"
- Sorteio: rota pública /sorteio/<id>/telao sem login
- Camisetas: página pública /camisetas sem login, acesso por CPF

## 👥 Perfis

- admin      → acesso total
- operador   → disparo, relatórios, templates
- visualizador → só dashboard e relatórios

## ⚙️ Comandos úteis

systemctl restart bingo
systemctl status bingo
journalctl -u bingo -f

## 📝 Convenções

- Telefones: sanitizar_telefone() — sem DDI, 11 dígitos
- Status: primeira letra maiúscula (Pendente, Pago, Disponivel)
- Valores: string "R$ X.XXX,XX"
- Lock banco: _db_lock (RLock) + get_db()

## 🔄 Versão: v2.6.0
