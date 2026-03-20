# enki.storage

Storage Superset for Minio.IO and S3 Libraries!

## Nota da versão 1.0.16

Nesta versão estamos atualizando o Minio.io da versão RELEASE.2021-02-24T18-44-45Z para a versão RELEASE.2025-09-07T16-13-09Z, que é a versão mais recente disponível no momento.

Isso pode trazer problemas de compatibilidade que estamos trabalhando para resolver e evitar causar mais problemas.

## Como rodar os testes

- Subir os containers de S3 e minIO
```bash
docker compose up -d
```

- Criar o arquivo appsettings.Development.json no (./test/enki.storage.test/config)
```json
{
    "Minio": {
        "EndPoint": "localhost:9000",
        "AccessKey": "enkiminiodev",
        "SecretKey": "secretminiodevelopmentenvironment2018",
        "Secure": false,
        "DefaultBucket": "enki.storage.test-minio-us-east-1",
        "Region": "us-east-1"
    },
    "S3": {
        "EndPoint": "http://localhost:4566",
        "AccessKey": "test",
        "SecretKey": "tes",
        "Secure": false,
        "DefaultBucket": "enki.storage.test-s3-us-east-1",
        "Region": "us-east-1"
    }
}

```

- Executar o comando abaixo em um terminal:
```bash
make run-test
```