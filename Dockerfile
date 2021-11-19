# Multi Stage Build
# 前半で requirements.txt を作成、後半でそれを用いて Python アプリケーションの載った Docker Image を作成
# poetry.lock を読むためには Poetry をインストールする必要がある一方、
# アプリケーションが載るコンテナには Poetry を入れておく必要がないため、これらを切り離したいという意図

# 前半
# Docker のキャッシュ戦略をうまく使うため、一番最初に Poetry をインストール。
# こうすることで、ビルドのたびに Poetry をインストールする手間が省ける。
# 次に、pyproject.toml と poetry.lock だけをコピーして、requirements.txt を生成
FROM python:3.9-slim as builder

WORKDIR /usr/src/app

RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN poetry export -f requirements.txt > requirements.txt

# 後半
# できあがった requirements.txt を前半部分からコピーしてきた上で、
# pip コマンドを使ってそれらのパッケージを全てインストール。
# もしこれらのファイルに変更がない場合はキャッシュが使われて自動で次のレイヤのビルドが行われるので、
# ビルドのたびに依存パッケージをダウンロードしてくるような動作を事前に防止することができる。
# これで圧倒的にビルド時間を節約できる。インストールが終わった後に、各種 Python スクリプトを転送して、動かしたいコマンドなどを書いて完成
FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/requirements.txt .

RUN pip install -r requirements.txt

COPY ./app /usr/src/app

RUN apt-get update
RUN apt-get upgrade -y

# RUN apt install -y curl

# RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /usr/src/app/google-cloud-sdk.tar.gz  \
#     && mkdir -p /usr/local/gcloud \
#     && tar -C /usr/local/gcloud -xvf /usr/src/app/google-cloud-sdk.tar.gz \
#     && /usr/local/gcloud/google-cloud-sdk/install.sh
# ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

EXPOSE 8080
CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080" ]