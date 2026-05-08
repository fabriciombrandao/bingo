# BINGO WHATSAPP — Sistema de Cobrança
## Contexto do Projeto para IA (Claude)

---

## 🗂 Estrutura do Projeto

```
/opt/bingo/
├── app.py                  # Backend Flask principal
├── templates/              # HTMLs (login, index, cadastro, camisetas, telao)
├── templates_msg/          # Templates de mensagem WhatsApp (.json)
├── static/imagens/         # Banners e rodapés dos templates
├── update.sh               # Script de deploy: git pull + systemctl restart bingo
└── venv/                   # Ambiente virtual Python
```

---

## 🚀 Stack

- Backend: Python 3.10 + Flask
- Banco: SQLite (WAL mode)
- WhatsApp: Twilio API
- Frontend: HTML/CSS/JS puro (index.html ~7000 linhas)
- Servidor: VPS Ubuntu — /opt/bingo — porta 60080
- Serviço: bingo.service (systemd)
- GitHub: https://github.com/fabriciombrandao/bingo

---

## 🔄 Fluxo de Desenvolvimento

1. Claude clona o repo com token no início de cada sessão
2. Claude edita os arquivos e faz `git push origin main`
3. Fabricio roda `/opt/bingo/update.sh` no servidor
4. Sistema atualizado ✅

### Início de sessão (Claude faz isso automaticamente)
```bash
git clone https://TOKEN@github.com/fabriciombrandao/bingo.git /tmp/bingo
cd /tmp/bingo
```

---

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

---

## 📋 Campos importantes de contatos

status: Disponivel | Pendente | Pago | Desmembrado
origem_id: NULL=lote normal, preenchido=cartela desmembrada

---

## 🔑 Regras de Negócio

- Só dispara WhatsApp para status=Pendente
- Previsão de pagamento futura = não dispara
- dias_disparo=0 dispara todos os pendentes
- Nunca dispara 2x no mesmo dia para o mesmo contato
- Desmembramento: lote Disponivel com intervalo "X a Y"
- Sorteio: rota pública /sorteio/<id>/telao sem login
- Camisetas: página pública /camisetas sem login, acesso por CPF

---

## 👥 Perfis

- admin      → acesso total
- operador   → disparo, relatórios, templates
- visualizador → só dashboard e relatórios

---

## ⚙️ Comandos úteis no servidor

```bash
systemctl restart bingo
systemctl status bingo
journalctl -u bingo -f
/opt/bingo/update.sh     # deploy: git pull + restart
```

---

## 📝 Convenções

- Telefones: sanitizar_telefone() — sem DDI, 11 dígitos
- Status: primeira letra maiúscula (Pendente, Pago, Disponivel)
- Valores: string "R$ X.XXX,XX"
- Lock banco: _db_lock (RLock) + get_db()

---

## 🔄 Versão: v2.6.0

---

## 📅 Histórico de Sessões

### Sessão 08/05/2026
- Repositório GitHub criado: https://github.com/fabriciombrandao/bingo
- .gitignore configurado (exclui db, config, credenciais, logs)
- CONTEXT.md criado com arquitetura completa do sistema
- Primeiro commit: 417fab2 — sistema bingo v2.6.0
- update.sh criado em /opt/bingo para deploy automático
- Fluxo de atualização definido: Claude clona repo → edita → push → VPS roda bingo-update
- Servidor confirmado sincronizado com GitHub ✅
- Definido que Claude consulta e atualiza CONTEXT.md direto no GitHub a cada sessão (mesmo padrão TNORTEANDO)
- update.sh atualizado com backup automático do banco antes de cada deploy (mantém últimos 7 em /opt/bingo/backups/)
- Criado symlink `bingo-update` em /usr/local/bin — roda de qualquer lugar no terminal
- Implementado cancelamento de desmembramento de lotes:
  - Backend: POST /api/contatos/<id>/cancelar-desmembramento
  - Valida que todas as cartelas filhas estão com status Disponivel
  - Se não estiver, retorna mensagem detalhando quais impedem o cancelamento
  - Remove cartelas filhas e restaura lote original para Disponivel
  - Registra na auditoria
  - Frontend: modal de desmembrar exibe seção extra com lotes desmembrados e botão ↩ Cancelar
