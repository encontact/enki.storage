using enki.storage.Interface;
using Minio;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.DataModel.Result;
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
        private IMinioClient _minioClient;
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
            _minioClient = new MinioClient().WithEndpoint(ServerConfig.EndPoint).WithCredentials(ServerConfig.AccessKey, ServerConfig.SecretKey).Build();
        }

        /// <summary>
        /// Valida se um balde existe de forma assincrona.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser pesquisada.</param>
        /// <returns>Tarefa indicando sucesso ou falha ao terminar.</returns>
        public override async Task<bool> BucketExistsAsync(string bucketName)
        {
            ValidateInstance();
            var args = new BucketExistsArgs().WithBucket(bucketName);
            return await _minioClient.BucketExistsAsync(args).ConfigureAwait(false);
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
                var argsWithRegions = new MakeBucketArgs()
                    .WithBucket(bucketName)
                    .WithLocation(ServerConfig.Region);

                await _minioClient.MakeBucketAsync(argsWithRegions).ConfigureAwait(false);
                return;
            }

            var args = new MakeBucketArgs().WithBucket(bucketName);
            await _minioClient.MakeBucketAsync(args).ConfigureAwait(false);
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor, especificamente numa região.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public override async Task MakeBucketAsync(string bucketName, string region)
        {
            ValidateInstance();
            var args = new MakeBucketArgs()
                .WithBucket(bucketName)
                .WithLocation(region);
            await _minioClient.MakeBucketAsync(args).ConfigureAwait(false);
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
            var args = new RemoveBucketArgs().WithBucket(bucketName);
            await _minioClient.RemoveBucketAsync(args).ConfigureAwait(false);
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
            var args = new PresignedPutObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithExpiry(expiresInt);
            return await _minioClient.PresignedPutObjectAsync(args).ConfigureAwait(false);
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
                { "contentmd5", md5Hash }
            };
            var args = new PutObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithFileName(filePath)
                .WithContentType(contentType)
                .WithHeaders(metadata);

            await _minioClient.PutObjectAsync(args).ConfigureAwait(false);
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
                { "contentmd5", md5Hash }
            };
            var args = new PutObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithStreamData(data)
                .WithObjectSize(size)
                .WithContentType(contentType)
                .WithHeaders(metadata);

            await _minioClient.PutObjectAsync(args).ConfigureAwait(false);
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
            var args = new RemoveObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName);
            await _minioClient.RemoveObjectAsync(args).ConfigureAwait(false);
        }

        public override async Task RemoveObjectsAsync(string bucketName, IEnumerable<string> objects)
        {
            foreach (var objectName in objects)
            {
                var args = new RemoveObjectArgs()
                    .WithBucket(bucketName)
                    .WithObject(objectName);
                await _minioClient.RemoveObjectAsync(args).ConfigureAwait(false);
            }
        }

        public override async Task<BatchDeleteProcessor> RemovePrefixAsync(
            string bucketName,
            string prefix,
            int chunkSize,
            CancellationToken cancellationToken = default)
        {
            ValidateInstance();

            var processor = new BatchDeleteProcessor(async (IEnumerable<string> keys) =>
            {
                // Monta os argumentos para remoção em lote
                var removeArgs = new RemoveObjectsArgs()
                    .WithBucket(bucketName)
                    .WithObjects(keys.ToList());

                var deleteErrors = await _minioClient
                    .RemoveObjectsAsync(removeArgs, cancellationToken)
                    .ConfigureAwait(false);

                foreach (var deleteError in deleteErrors)
                {
                    Console.WriteLine("Delete error for object: {0}", deleteError.Key);
                }
            });

            var bucketKeys = new List<string>();
            var prefixToFilter = prefix.EndsWith("/") ? prefix : prefix + "/";

            var listArgs = new ListObjectsArgs()
                .WithBucket(bucketName)
                .WithPrefix(prefixToFilter)
                .WithRecursive(true);

            var asyncEnumerable = _minioClient.ListObjectsEnumAsync(listArgs, cancellationToken);
            var enumerator = asyncEnumerable.GetAsyncEnumerator(cancellationToken);

            try
            {
                while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    var item = enumerator.Current;

                    bucketKeys.Add(item.Key);

                    if (bucketKeys.Count >= chunkSize)
                    {
                        processor.EnqueueChunk(bucketKeys);
                        bucketKeys = new List<string>();
                    }
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (bucketKeys.Any())
            {
                processor.EnqueueChunk(bucketKeys);
            }

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
        /// Recupera metadata de um objeto
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns>Dicionario com o metadata do objeto</returns>
        public override async Task<IDictionary<string, string>> GetObjectMetadataAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            var result = await StatObjectAsync(bucketName, objectName).ConfigureAwait(false);

            return result.MetaData;
        }

        /// <summary>
        /// Recupera todos os objetos do bucked ou a partir de um prefix de forma recursiva.
        /// </summary>
        /// <param name="bucketName">Bucket name</param>
        /// <param name="prefix">Opcional, prefixo raiz para pesquisa</param>
        /// <returns>Lista de arquivos encontrados, ignorando diretórios.</returns>
        public override async Task<IEnumerable<IObjectInfo>> ListObjectsAsync(string bucketName, string prefix = null)
        {
            ValidateInstance();

            var result = new List<IObjectInfo>();

            var args = new ListObjectsArgs()
                .WithBucket(bucketName)
                .WithRecursive(true);

            var asyncEnumerable = _minioClient.ListObjectsEnumAsync(args);
            var enumerator = asyncEnumerable.GetAsyncEnumerator();

            try
            {
                while (await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    var item = enumerator.Current;

                    if (!item.IsDir)
                        result.Add(new ObjectInfo(item));
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

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
            var args = new StatObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName);
            return await _minioClient.StatObjectAsync(args).ConfigureAwait(false);
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
                var args = new StatObjectArgs()
                    .WithBucket(bucketName)
                    .WithObject(objectName);
                await _minioClient.StatObjectAsync(args).ConfigureAwait(false);
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
            var args = new GetObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithCallbackStream(action);
            await _minioClient.GetObjectAsync(args).ConfigureAwait(false);
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
            var copySourceArgs = new CopySourceObjectArgs()
               .WithBucket(bucketName)
               .WithObject(objectName);

            var copyObjectArgs = new CopyObjectArgs()
               .WithBucket(destBucketName)
               .WithObject(destObjectName)
               .WithCopyObjectSource(copySourceArgs);

            await _minioClient.CopyObjectAsync(copyObjectArgs).ConfigureAwait(false);
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
            var args = new PresignedGetObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithExpiry(expiresInt);
            return await _minioClient.PresignedGetObjectAsync(args).ConfigureAwait(false);
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

        // O SDK do minio já lida com o multipart upload internamente.
        public override async Task<PutObjectResponse> MultipartUploadAsync(
            string bucketName,
            string objectName,
            Stream data,
            string contentType,
            int partSize = 5 * 1024 * 1024,
            CancellationToken cancellationToken = default
        )
        {
            ValidateInstance();

            if (data == null)
                throw new ArgumentNullException(nameof(data));

            if (data.CanSeek)
                data.Seek(0, SeekOrigin.Begin);

            var args = new PutObjectArgs()
                .WithBucket(bucketName)
                .WithObject(objectName)
                .WithStreamData(data)
                .WithContentType(contentType);

            if (data.CanSeek)
                args = args.WithObjectSize(data.Length);

            await _minioClient
                .PutObjectAsync(args, cancellationToken)
                .ConfigureAwait(false);

            return new PutObjectResponse(true);
        }
    }
}
