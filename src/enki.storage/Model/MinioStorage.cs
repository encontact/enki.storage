﻿using enki.storage.Interface;
using Minio;
using Minio.DataModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace enki.storage.Model
{
    public class MinioStorage : BaseStorage
    {
        private MinioClient _minioClient;
        public bool UseRegion => ServerConfig.MustConnectToRegion();

        public MinioStorage(IStorageServerConfig config) : base(config) { }

        /// <summary>
        /// Efetua a conexão com o servidor Minio/S3 a partir dos dados do construtor.
        /// </summary>
        public override void Connect()
        {
            if (_minioClient != null) return;

            // TODO: Ao utilizar region na conexão, o sistema apresenta a falha abaixo, por este motivo, a region deve ser informada 
            //       nos outros pontos, porém não deve ser informada na conexão.
            // Issue relatando caso: https://github.com/minio/minio-js/issues/619
            // Notar que exemplo de connect no GitHub do Minio.DotNet não inclui region no construtor, mas apresenta nas chamadas de bucket.
            // Devido a estes pontos, não é efetuada a verificação MustConnectToRegion() da interface de configuração.
            _minioClient = new MinioClient(ServerConfig.EndPoint, ServerConfig.AccessKey, ServerConfig.SecretKey);
        }

        /// <summary>
        /// Valida se um balde existe de forma assincrona.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser pesquisada.</param>
        /// <returns>Tarefa indicando sucesso ou falha ao terminar.</returns>
        public override async Task<bool> BucketExistsAsync(string bucketName)
        {
            ValidateInstance();
            return await _minioClient.BucketExistsAsync(bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Valida se um balde existe de forma assincrona.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser pesquisada.</param>
        /// <returns>Tarefa indicando sucesso ou falha ao terminar.</returns>
        public bool BucketExists(string bucketName)
        {
            ValidateInstance();
            return _minioClient.BucketExistsAsync(bucketName).Result;
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public override async Task MakeBucketAsync(string bucketName)
        {
            ValidateInstance();
            if (UseRegion)
            {
                await _minioClient.MakeBucketAsync(bucketName, ServerConfig.Region).ConfigureAwait(false);
                return;
            }

            await _minioClient.MakeBucketAsync(bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor, especificamente numa região.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public override async Task MakeBucketAsync(string bucketName, string region)
        {
            ValidateInstance();
            await _minioClient.MakeBucketAsync(bucketName, ServerConfig.Region).ConfigureAwait(false);
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public async Task<ListAllMyBucketsResult> ListAllAsync()
        {
            ValidateInstance();
            return await _minioClient.ListBucketsAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Exclui um balde no servidor
        /// </summary>
        /// <param name="bucketName">Nome do balde a ser removido</param>
        public override async Task RemoveBucketAsync(string bucketName)
        {
            ValidateInstance();
            await _minioClient.RemoveBucketAsync(bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua a criação de uma URL temporária para upload de anexo sem depender de autenticação.
        /// Util para performar os uploads tanto de anexos como de imagens no corpo efetuadas pela plataforma.
        /// </summary>
        /// <param name="bucketName">Bucket onde será inserido o registro.</param>
        /// <param name="objectName">Nome/Caminho do objeto a ser inserido.</param>
        /// <param name="expiresInt">Tempo em segundos no qual a url será valida para o Upload.</param>
        /// <returns></returns>
        public override async Task<string> PresignedPutObjectAsync(string bucketName, string objectName, int expiresInt, string contentMD5 = null)
        {
            ValidateInstance();
            return await _minioClient.PresignedPutObjectAsync(bucketName, objectName, expiresInt).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua o upload de um arquivo a partir do servidor.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <param name="filePath">Caminho do arquivo no servidor</param>
        /// <param name="contentType">Tipo do conteúdo do arquivo</param>
        /// <returns>Tarefa em execução.</returns>
        public override async Task<PutObjectResponse> PutObjectAsync(string bucketName, string objectName, string filePath, string contentType)
        {
            ValidateInstance();

            var md5Hash = new CreateMD5CheckSum(filePath).GetMd5();
            var metadata = new Dictionary<string, string>
            {
                { "ContentMD5", md5Hash }
            };
            await _minioClient.PutObjectAsync(bucketName, objectName, filePath, contentType, metaData: metadata).ConfigureAwait(false);
			return new PutObjectResponse(true);
        }

        /// <summary>
        /// Efetua o upload de um arquivo a partir de um Stream em memória.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <param name="data">Stream com conteúdo</param>
        /// <param name="size">Tamanho do conteúdo</param>
        /// <param name="contentType">Tipo do conteudo</param>
        /// <returns>Tarefa em execução.</returns>
        public override async Task<PutObjectResponse> PutObjectAsync(string bucketName, string objectName, Stream data, long size, string contentType)
        {
            ValidateInstance();

            var md5Hash = new CreateMD5CheckSum(data).GetMd5();
            data.Seek(0, SeekOrigin.Begin);
            var metadata = new Dictionary<string, string>
            {
                { "ContentMD5", md5Hash }
            };
            await _minioClient.PutObjectAsync(bucketName, objectName, data, size, contentType, metaData: metadata).ConfigureAwait(false);
			return new PutObjectResponse(true);
        }

        /// <summary>
        /// Remove um arquivo contido num balde
        /// </summary>
        /// <param name="bucketName">Nome do balde onde o arquivo se encontra.</param>
        /// <param name="objectName">Nome do objeto a ser removido.</param>
        /// <returns>Tarefa em execução.</returns>
        public override async Task RemoveObjectAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            await _minioClient.RemoveObjectAsync(bucketName, objectName).ConfigureAwait(false);
        }

        public override async Task RemoveObjectsAsync(string bucketName, IEnumerable<string> objects)
        {
            foreach (var objectName in objects)
            {
                await _minioClient.RemoveObjectAsync(bucketName, objectName).ConfigureAwait(false);
            }
        }

        public override async Task<BatchDeleteProcessor> RemovePrefixAsync(string bucketName, string prefix, int chunkSize, CancellationToken cancellationToken = default)
        {
            ValidateInstance();

            var processor = new BatchDeleteProcessor(async (IEnumerable<string> keys) =>
            {
                var finishedDelete = false;
                var observableDelete = await _minioClient.RemoveObjectAsync(bucketName, keys, cancellationToken).ConfigureAwait(false);
                // Remove list of objects in objectNames from the bucket bucketName.
                observableDelete.Subscribe(
                    deleteError => Console.WriteLine("Object: {0}", deleteError.Key),
                    ex => Console.WriteLine("OnError: {0}", ex),
                    () => finishedDelete = true
                );

                while (!finishedDelete)
                    await Task.Delay(250);
            });

            var bucketKeys = new List<string>();
            var finishedList = false;
            var prefixToFilter = (prefix.EndsWith("/") ? prefix : prefix + "/");
            var observable = _minioClient.ListObjectsAsync(bucketName, prefixToFilter, true, cancellationToken);
            observable.Subscribe
            (
                item =>
                {
                    bucketKeys.Add(item.Key);
                    if (bucketKeys.Count >= chunkSize)
                    {
                        processor.EnqueueChunk(bucketKeys);
                        bucketKeys = new List<string>();
                    }
                },
                () =>
                {
                    if (bucketKeys.Any())
                        processor.EnqueueChunk(bucketKeys);

                    finishedList = true;
                }
            );

            while (!finishedList) await Task.Delay(100);

            return processor;
        }

        /// <summary>
        /// Recupera estatisticas do objecto solicitado.
        /// </summary>
        /// <param name="bucketName">Nome do bucket</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>Recupera as informações do objeto.</returns>
        public override async Task<IObjectInfo> GetObjectInfoAsync(string bucketName, string objectName)
            => new ObjectInfo(await StatObjectAsync(bucketName, objectName).ConfigureAwait(false));

        /// <summary>
        /// Recupera todos os objetos do bucked ou a partir de um prefix de forma recursiva.
        /// </summary>
        /// <param name="bucketName">Bucket name</param>
        /// <param name="prefix">Opcional, prefixo raiz para pesquisa</param>
        /// <returns>Lista de arquivos encontrados, ignorando diretórios.</returns>
        public override async Task<IEnumerable<IObjectInfo>> ListObjectsAsync(string bucketName, string prefix = null)
        {
            ValidateInstance();
            var finishedList = false;
            var result = new List<IObjectInfo>();

            var items = _minioClient.ListObjectsAsync(bucketName, prefix, true);
            items.Subscribe
            (
                item =>
                {
                    if (!item.IsDir)
                        result.Add(new ObjectInfo(item));
                },
                () =>
                {
                    finishedList = true;
                }
            );

            while (!finishedList)
                await Task.Delay(100);

            return result;
        }

        /// <summary>
        /// Valida se um objeto existe ou não no balde.
        /// Se o arquivo não existir no servidor, será retornada uma Exception.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>Tarefa com o status do objeto.</returns>
        public async Task<ObjectStat> StatObjectAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            return await _minioClient.StatObjectAsync(bucketName, objectName).ConfigureAwait(false);
        }

        /// <summary>
        /// Valida se um objeto existe ou não no balde.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>True se existe e False se não existe.</returns>
        public override async Task<bool> ObjectExistAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            try
            {
                await _minioClient.StatObjectAsync(bucketName, objectName).ConfigureAwait(false);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Recupera um objeto do balde.
        /// </summary>
        /// <param name="bucketName">Nome do balde.</param>
        /// <param name="objectName">Nome do objeto a ser recuperado.</param>
        /// <param name="action">Função de callback com a Stream recuperada do servidor.</param>
        public override async Task GetObjectAsync(string bucketName, string objectName, Action<Stream> action)
        {
            ValidateInstance();
            await _minioClient.GetObjectAsync(bucketName, objectName, action).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua a copia de um objeto no servidor, evitando a necessidade de efetuar um upload.
        /// </summary>
        /// <param name="bucketName">Nome do balde de origem da copia.</param>
        /// <param name="objectName">Nome do objecto de origem da copia.</param>
        /// <param name="destBucketName">Balde de destino</param>
        /// <param name="destObjectName">Objeto de destino</param>
        /// <returns>Tarefa sendo executada.</returns>
        public override async Task CopyObjectAsync(string bucketName, string objectName, string destBucketName, string destObjectName)
        {
            ValidateInstance();
            await _minioClient.CopyObjectAsync(bucketName, objectName, destBucketName, destObjectName).ConfigureAwait(false);
        }

        /// <summary>
        /// Obtém uma url de acesso temporário ao anexo.
        /// </summary>
        /// <param name="bucketName">Nome do balde de origem da copia.</param>
        /// <param name="objectName">Nome do objecto de origem da copia.</param>
        /// <param name="expiresInt">Tempo de expiração em segundos.</param>
        /// <param name="reqParams">Parametros adicionais do Header a serem utilizados. Suporta os Headers: response-expires, response-content-type, response-cache-control, response-content-disposition</param>
        /// <returns>Url para obtenção do arquivo.</returns>
        public override async Task<string> PresignedGetObjectAsync(string bucketName, string objectName, int expiresInt, Dictionary<string, string> reqParams = null)
        {
            ValidateInstance();
            return await _minioClient.PresignedGetObjectAsync(bucketName, objectName, expiresInt, reqParams).ConfigureAwait(false);
        }

        /// <summary>
        /// Ativa regra de CORS no Bucket para permitir acesso externo via javascript.
        /// NOTA: Este método não tem função para o Minio.
        /// </summary>
        /// <param name="bucketName">Nome do Bucket para adicionar a regra de CORS</param>
        /// <param name="allowedOrigin">Origigem a ser ativada.</param>
        /// <returns></returns>
        public override async Task SetCorsToBucketAsync(string bucketName, string allowedOrigin) => await Task.Run(() => { }).ConfigureAwait(false);

        /// <summary>
        /// Indica se deve lidar o trace para os comandos do mínio.
        /// </summary>
        /// <param name="situation">True para ligar o trace e False para desligar.</param>
        public void SetTrace(bool situation)
        {
            ValidateInstance();

            if (situation) _minioClient.SetTraceOn();
            _minioClient.SetTraceOff();
        }
        /// <summary>
        /// Efetua a validação se o usuário efetuou a conexão antes de executar as ações.
        /// </summary>
        private void ValidateInstance()
        {
            if (_minioClient == null)
                throw new ObjectDisposedException("Não foi efetuada conexão com o servidor. Utilize a função Connect() antes de chamar as ações.");
        }
    }
}
