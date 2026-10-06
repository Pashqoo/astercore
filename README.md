# Astercore

Ядро биржи [Aster](https://www.asterdex.com) для MoonTerminal по протоколу MoonProto: маркет-дата,
счёт, ордера и движок стратегий MoonBot. Постановка, замеры и решения — [`PLAN.md`](PLAN.md).

## Установка на Linux

Команды — для Debian/Ubuntu; юнит подойдёт любому Linux с systemd. Прогнана 04.10 на Debian 13
(2 ядра, 1.9 ГБ памяти + swap): сборка ~4.5 мин, тесты зелёные. Toolchain — 1.98.0: на 1.99
сборка идёт, но с двумя предупреждениями об устаревшем `fetch_update`.
Всё чистый Rust на rustls: OpenSSL не нужен, нужен только компилятор C для `ring`.

### 1. Сборка

```sh
sudo apt update && sudo apt install -y build-essential git curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"

git clone https://github.com/Pashqoo/astercore.git
cd astercore
cargo build --release -p aster-core
```

Бинарь — `target/release/aster-core`, самодостаточный (шрифт графиков вшит). Можно собрать на
другой машине той же архитектуры и скопировать только его.

### 2. Рабочий каталог

Ядро держит всё своё в **рабочем каталоге**: `aster-core.key` (ключ терминала), `data/`
(настройки, стратегии, отчёты), `logs/` (журнал по дням). Юнит ждёт `/opt/aster-core`:

Служба идёт от отдельного пользователя без прав на остальную систему (`User=aster` в юните):

```sh
sudo useradd --system --home-dir /opt/aster-core --no-create-home --shell /usr/sbin/nologin aster
sudo mkdir -p /opt/aster-core
sudo install -m 755 target/release/aster-core /opt/aster-core/
sudo chown -R aster:aster /opt/aster-core
```

### 3. Ключ биржи

Файл `/opt/aster-core/asterkey` — API-кошелёк Aster (подпись v3, EIP-712): приватный ключ
(64 hex) и адрес основного счёта (40 hex), с `0x` или без, в любой разметке — подписи вида
`key=`, `user:` игнорируются. Права только владельцу:

```sh
sudo install -m 600 -o aster -g aster /dev/null /opt/aster-core/asterkey
sudo nano /opt/aster-core/asterkey
```

Без файла ядро работает только с маркет-датой, стратегии — только в эмуляторе. Testnet —
`ASTER_NET=testnet` и отдельный ключ с `asterdex-testnet.com`.

### 4. Первый запуск — вручную, ради ключа терминала

Ключ терминала выпускается при первом старте и печатается в stdout — **только на консоль**
(в канал или в журнал он не попадает). В нём записан адрес, по которому терминал будет звонить,
поэтому на сервере задайте **внешний IP** хоста:

```sh
cd /opt/aster-core
sudo -u aster ASTER_CORE_ADDR=<внешний IP> ASTER_API_KEY_FILE=/opt/aster-core/asterkey ./aster-core
```

Вторая строка вывода — ключ для импорта в MoonTerminal. Дождитесь `serving` в журнале,
остановите `Ctrl+C`. Ключ лёг в `aster-core.key` (права 600) и дальше переиспользуется; адрес
сменить — удалить файл и выпустить заново (терминал придётся переимпортировать).

### 5. Служба systemd

```sh
sudo cp tools/aster-core.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aster-core
journalctl -u aster-core -f
```

Коды выхода и политика рестарта описаны в самом юните: стоп со страницы или `systemctl stop` —
служба остаётся остановленной, рестарт со страницы или сбой — поднимается снова.

Под systemd stdout — journald, поэтому ключ терминала в него **не печатается** (journald хранит
строки навсегда и показывает группам `adm`/`systemd-journal`). Нужен снова — из каталога ядра:
`sudo -u aster ./aster-core --print-key` (читает `aster-core.key`, ничего не запускает).

### 6. Сеть

| порт | протокол | кто | по умолчанию |
|---|---|---|---|
| 3101 | **UDP** | MoonTerminal → ядро | открыт наружу; `ASTER_CORE_PORT` |
| 3102 | TCP | страница оператора | только `127.0.0.1` |

```sh
sudo ufw allow 3101/udp
```

Страницу удобнее смотреть через SSH-туннель: `ssh -L 3102:127.0.0.1:3102 <хост>`, затем
`http://127.0.0.1:3102`. Вывести её на внешний адрес (`web.bind` на странице) ядро позволит
только с заданным паролем страницы.

### 7. Проверка

- `journalctl -u aster-core` — строки `clock:`, `catalog:`, `account:` (баланс USDT и позиции),
  `serving`;
- терминал с импортированным ключом доходит до списка рынков;
- `curl -H 'X-Astercore: 1' http://127.0.0.1:3102/api/status` на самом хосте.

### Переменные окружения

| переменная | смысл | по умолчанию |
|---|---|---|
| `ASTER_API_KEY_FILE` | файл ключа биржи; явно названный и отсутствующий — ошибка старта | `asterkey` в рабочем каталоге |
| `ASTER_NET` | `mainnet` / `testnet` | `mainnet` |
| `ASTER_CORE_PORT` | UDP-порт для терминала | `3101` |
| `ASTER_CORE_ADDR` | адрес в **новом** ключе терминала | нет — терминал звонит на `127.0.0.1` |
| `ASTER_CORE_LOG` | уровень журнала (`error`…`trace`) | `info` |

Остальное — токен Telegram-бота, пароль страницы, автозапуск стратегий — настраивается на
странице и хранится в `data/config.json` (права 600).

### Обновление

Вручную:

```sh
cd astercore && git pull
ASTER_CORE_BUILD=$(git rev-list --count HEAD) ASTER_CORE_COMMIT=$(git rev-parse HEAD) \
  cargo build --release -p aster-core
sudo systemctl stop aster-core
sudo install -m 755 target/release/aster-core /opt/aster-core/
sudo systemctl start aster-core
```

`stop` штатный: ядро снимает входы с биржи и сохраняет состояние (до 40 с). Ключ терминала,
настройки и стратегии в `/opt/aster-core` сохраняются.

`ASTER_CORE_BUILD` — номер сборки (число коммитов `main`): его терминал показывает как версию
ядра (`1.20` для 120) и сравнивает до и после обновления. Сборка без него сообщает `0.01`.

### Обновление из терминала

Кнопка «Обновить» в окне статуса ядер MoonTerminal («до релиза» или «до именованной версии»)
обновляет ядро из исходников на GitHub. «Релиз» — последний коммит `main`. Именованная версия —
коммит (7–40 знаков hex) или тег, и только если он есть в `main`: ветки и чужие ссылки не
собираются. Ядро отказывает, пока у него открыта позиция или уже идёт обновление.

Как это работает: ядро само ничего не собирает. Оно пишет заявку `data/update-request`, а
служба `aster-core-update` от root (её запускает path-юнит) делает `git fetch`, собирает с
пониженным приоритетом строго по `Cargo.lock` коммита (`--locked`: лок, разошедшийся с
`Cargo.toml`, — проваленная сборка, а не новое разрешение зависимостей с crates.io), прогоняет
контрактный тест и кладёт бинарь рядом как `aster-core.next`.
Ядро подменяет им себя, как только у него нет позиций (ждёт до 10 минут, потом отменяет), и
перезапускается (код 70). Старый бинарь остаётся как `aster-core.prev`. Если новое ядро не
поднялось за 300 с, служба возвращает старый бинарь и перезапускает ядро. Каждый шаг пишется в
журнал ядра, а итог — ещё и в Telegram.

Установка, один раз, **в shell от root** (служба собирает тулчейном root из `/root/.cargo` и
ходит на GitHub его ssh-ключом; `sudo` здесь не годится — он теряет `~/.cargo/bin` из `PATH`):

```sh
install -m 755 tools/update.sh /usr/local/sbin/aster-core-update
install -m 644 tools/aster-core-update.service tools/aster-core-update.path /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now aster-core-update.path
# Прогрев: первый клон и полная сборка (~5 мин), чтобы кнопка потом собирала только ядро.
mkdir -p -m 755 /var/lib/aster-core-update
git clone git@github.com:Pashqoo/astercore.git /var/lib/aster-core-update/src
cd /var/lib/aster-core-update/src && CARGO_TARGET_DIR=/var/lib/aster-core-update/target \
  nice -n 15 cargo build --locked --release -p aster-core
```

Журнал обновления — `journalctl -u aster-core-update`, вывод сборки —
`/var/lib/aster-core-update/build.log`. Скрипт сам себя не обновляет: правка `tools/update.sh`
или юнитов ставится только вручную, командами выше.
