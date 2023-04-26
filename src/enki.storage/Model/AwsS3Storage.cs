using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using enki.storage.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace enki.storage.Model
{
    public class AwsS3Storage : BaseStorage
    {
        private IAmazonS3 _client { get; set; }
        public static bool IsAmazonS3Config(IStorageServerConfig config) => config.EndPoint.ToUpper().Trim() == "S3.AMAZONAWS.COM";
        public bool IsAmazonS3Config() => IsAmazonS3Config(ServerConfig);
        public bool UseRegion => !string.IsNullOrWhiteSpace(ServerConfig.Region);

        public AwsS3Storage(IStorageServerConfig config) : base(config)
        {
            if (!IsAmazonS3Config()) throw new ArgumentException("Endpoint is not valid AWS S3.");
        }

        /// <summary>
        /// Efetua a conexão com o servidor Minio/S3 a partir dos dados do construtor.
        /// </summary>
        public override void Connect()
        {
            if (_client != null) return;
            var credentials = new BasicAWSCredentials(ServerConfig.AccessKey, ServerConfig.SecretKey);

            // OBS: Essa configuração é feita pois por padrão, exclusivamente quando a região utilizada é a 
            //      us-east-1, utiliza a assinatura "version 2", com isso as "Presigned URLs" geradas não
            //      são formadas com todos parâmetros e acaba gerando erro 403 ao fazer upload pelo browser. 
            //      Mais informações através do comentário na propriedade 'AWSConfigsS3.UseSignatureVersion4'.
            AWSConfigsS3.UseSignatureVersion4 = true;

            // TODO: Ação para permitir que haja ações Inter-Regiões:
            // https://stackoverflow.com/questions/50289688/s3-copyobjectrequest-between-regions
            if (ServerConfig.MustConnectToRegion())
                _client = new AmazonS3Client(credentials, RegionEndpoint.GetBySystemName(ServerConfig.Region));
            else
                _client = new AmazonS3Client(credentials);
        }

        /// <summary>
        /// Valida se um balde existe de forma assincrona.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser pesquisada.</param>
        /// <returns>Tarefa indicando sucesso ou falha ao terminar.</returns>
        public override async Task<bool> BucketExistsAsync(string bucketName)
        {
            ValidateInstance();

            /*
            NOTA: Esse código corresponde ao método "DoesS3BucketExistV2Async" que substitui o
                  método obsoleto "DoesS3BucketExistAsync". Porém, até o dia  dessa alteração 
                  não tinha sido liberado o pacote nuget contendo essa melhoria, então replicamos o código aqui. 
                  Obs: Futuramente passar a utilizar o método 'DoesS3BucketExistV2Async'.
            */
            try
            {
                await _client.GetACLAsync(bucketName).ConfigureAwait(false);
            }
            catch (AmazonS3Exception e)
            {
                switch (e.ErrorCode)
                {
                    // A redirect error or a forbidden error means the bucket exists.
                    case "AccessDenied":
                    case "PermanentRedirect":
                        return true;
                    case "NoSuchBucket":
                        return false;
                    default:
                        throw;
                }
            }

            return true;
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public override async Task MakeBucketAsync(string bucketName)
        {
            ValidateInstance();
            await _client.PutBucketAsync(bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Cria um balde de forma assincrona no servidor, especificamente numa região.
        /// </summary>
        /// <param name="bucketName">Nome da carteira a ser criada.</param>
        public override async Task MakeBucketAsync(string bucketName, string region)
        {
            ValidateInstance();

            var request = new PutBucketRequest()
            {
                BucketName = bucketName,
                BucketRegionName = region,
                UseClientRegion = false,
            };
            await _client.PutBucketAsync(request).ConfigureAwait(false);
        }

        /// <summary>
        /// Ativa regra de CORS no Bucket para permitir acesso externo via javascript.
        /// </summary>
        /// <param name="bucketName">Nome do Bucket para adicionar a regra de CORS</param>
        /// <param name="allowedOrigin">Origigem a ser ativada.</param>
        /// <returns></returns>
        public override async Task SetCorsToBucketAsync(string bucketName, string allowedOrigin)
        {
            ValidateInstance();

            // Remove configuração CORS
            var requestDelete = new DeleteCORSConfigurationRequest
            {
                BucketName = bucketName
            };
            await _client.DeleteCORSConfigurationAsync(requestDelete);

            // Cria nova configuração CORS
            var configuration = new CORSConfiguration
            {
                Rules = new List<CORSRule>
                {
                      new CORSRule
                      {
                            Id = "enContactPutByJavascriptRule",
                            AllowedMethods = new List<string> { "PUT" },
                            AllowedHeaders = new List<string> { "*" },
                            AllowedOrigins = new List<string> { allowedOrigin ?? "*" }
                      },
                }
            };
            var requestCreate = new PutCORSConfigurationRequest
            {
                BucketName = bucketName,
                Configuration = configuration
            };
            await _client.PutCORSConfigurationAsync(requestCreate).ConfigureAwait(false);
        }

        /// <summary>
        /// Este método é específico para AWS e recupera a configuração do CORS existente.
        /// </summary>
        /// <returns>Recupera a configuração existente do CORS</returns>
        public async Task<CORSConfiguration> RetrieveCORSConfigurationAsync(string bucketName)
        {
            var request = new GetCORSConfigurationRequest
            {
                BucketName = bucketName

            };
            var response = await _client.GetCORSConfigurationAsync(request);
            var configuration = response.Configuration;
            return configuration;
        }

        /// <summary>
        /// Exclui um balde no servidor
        /// </summary>
        /// <param name="bucketName">Nome do balde a ser removido</param>
        public override async Task RemoveBucketAsync(string bucketName)
        {
            ValidateInstance();
            await _client.DeleteBucketAsync(bucketName).ConfigureAwait(false);
        }

        /// <summary>
        /// Recupera estatisticas do objecto solicitado.
        /// </summary>
        /// <param name="bucketName">Nome do bucket</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>Recupera as informações do objeto.</returns>
        public override async Task<IObjectInfo> GetObjectInfoAsync(string bucketName, string objectName)
            => new ObjectInfo(objectName, await StatObjectAsync(bucketName, objectName).ConfigureAwait(false));

        /// <summary>
        /// Recupera todos os objetos do bucked ou a partir de um prefix de forma recursiva.
        /// A AWS limita esta chamada a 1.000 registros de retorno, para mais do que isso será
        /// necessário criar algo com paginação.
        /// </summary>
        /// <param name="bucketName">Bucket name</param>
        /// <param name="prefix">Opcional, prefixo raiz para pesquisa</param>
        /// <returns>Lista de arquivos encontrados, ignorando diretórios.</returns>
        public override async Task<IEnumerable<IObjectInfo>> ListObjectsAsync(string bucketName, string prefix = null)
        {
            ValidateInstance();
            var result = new List<IObjectInfo>();

            var request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = prefix,
            };
            var resultS3List = await _client.ListObjectsV2Async(request).ConfigureAwait(false);
            result = resultS3List.S3Objects.Select(o => (IObjectInfo)new ObjectInfo(o)).ToList();

            return result;
        }

        /// <summary>
        /// Valida se um objeto existe ou não no balde.
        /// Se o arquivo não existir no servidor, será retornada uma Exception.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>Tarefa com o status do objeto.</returns>
        public async Task<GetObjectMetadataResponse> StatObjectAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            return await _client.GetObjectMetadataAsync(bucketName, objectName).ConfigureAwait(false);
        }

        /// <summary>
        /// Recupera metadata de um objeto
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <returns>Dicionario com o metadata do objeto</returns>
        public override async Task<IDictionary<string, string>> GetObjectMetadataAsync(string bucketName, string objectName)
        {
            ValidateInstance();
            var result = await _client.GetObjectAsync(bucketName, objectName).ConfigureAwait(false);
            var metadata = new Dictionary<string, string>();
            var awsMetadataPrefix = "x-amz-meta-";
            foreach (var item in result.Metadata.Keys)
            {
                var key = item;
                if (key.StartsWith(awsMetadataPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    key = key.Substring(awsMetadataPrefix.Length);
                }

                metadata.Add(key, result.Metadata[key]);
            }

            return metadata;
        }

        /// <summary>
        /// Efetua a criação de uma URL temporária para upload de anexo sem depender de autenticação.
        /// Util para performar os uploads tanto de anexos como de imagens no corpo efetuadas pela plataforma.
        /// </summary>
        /// <param name="bucketName">Bucket onde será inserido o registro.</param>
        /// <param name="objectName">Nome/Caminho do objeto a ser inserido.</param>
        /// <param name="expiresInt">Tempo em segundos no qual a url será valida para o Upload.</param>
        /// <param name="contentMD5">Hash MD5 para validação do arquivo que será feito o upload.</param>
        /// <returns></returns>
        public override async Task<string> PresignedPutObjectAsync(string bucketName, string objectName, int expiresInt, string contentMD5 = null)
        {
            ValidateInstance();

            return await Task.Run(() =>
            {
                var request = new GetPreSignedUrlRequest
                {
                    BucketName = bucketName,
                    Key = objectName,
                    Verb = HttpVerb.PUT,
                    Expires = DateTime.UtcNow.AddSeconds(expiresInt)
                };

                if (!string.IsNullOrWhiteSpace(contentMD5))
                    request.Headers.ContentMD5 = contentMD5;

                return _client.GetPreSignedURL(request);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua o upload de um arquivo a partir do servidor.
		/// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <param name="filePath">Caminho do arquivo no servidor</param>
        /// <param name="contentType">Tipo do conteúdo do arquivo</param>
        /// <returns>Tarefa em execução.</returns>
        public override async Task<enki.storage.Model.PutObjectResponse> PutObjectAsync(string bucketName, string objectName, string filePath, string contentType)
        {
            ValidateInstance();
            var md5Check = new CreateMD5CheckSum(filePath);
            var request = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectName,
                FilePath = filePath,
                ContentType = contentType,
            };
            var md5Hash = md5Check.GetMd5();
            request.Headers.ContentMD5 = md5Hash;
            request.Metadata.Add("ContentMD5", md5Hash);
            Amazon.S3.Model.PutObjectResponse result = await _client.PutObjectAsync(request).ConfigureAwait(false);

            // Fazendo um DoubleCheck no MD5, informamos no Header pra AWS checar e efetuamos a validação no retorno também.
            // Response de sucesso deve ser HTTP = 200 (Mais em: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_ResponseSyntax)
            return new enki.storage.Model.PutObjectResponse(
                                            result.HttpStatusCode == System.Net.HttpStatusCode.OK
                                            && md5Check.Validate(result.ETag)
                        );
        }

        /// <summary>
        /// Efetua o upload de um arquivo a partir de um Stream em memória.
		/// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <param name="data">Stream com conteúdo</param>
        /// <param name="size">Tamanho do conteúdo</param>
        /// <param name="contentType">Tipo do conteudo</param>
        /// <returns>Tarefa em execução.</returns>
        public override async Task<enki.storage.Model.PutObjectResponse> PutObjectAsync(string bucketName, string objectName, Stream data, long size, string contentType)
        {
            ValidateInstance();
            var md5Check = new CreateMD5CheckSum(data);
            var request = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = objectName,
                InputStream = data,
                ContentType = contentType
            };
            request.Headers.ContentMD5 = md5Check.GetBase64Md5();
            request.Metadata.Add("contentmd5", md5Check.GetMd5());

            Amazon.S3.Model.PutObjectResponse result = await _client.PutObjectAsync(request).ConfigureAwait(false);

            // Fazendo um DoubleCheck no MD5, informamos no Header pra AWS checar e efetuamos a validação no retorno também.
            // Response de sucesso deve ser HTTP = 200 (Mais em: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_ResponseSyntax)
            return new enki.storage.Model.PutObjectResponse(
                                            result.HttpStatusCode == System.Net.HttpStatusCode.OK
                                            && md5Check.Validate(result.ETag.Trim('"'))
                        );
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
            await _client.DeleteObjectAsync(bucketName, objectName).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua a exclusão em lote e vários objetos ao mesmo tempo.
        /// </summary>
        /// <param name="bucketName">Nome do bucket para remover.</param>
        /// <param name="objects">Lista de objetos a serem apagados</param>
        /// <returns></returns>
        public override async Task RemoveObjectsAsync(string bucketName, IEnumerable<string> objects)
        {
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest
            {
                BucketName = bucketName,
                Objects = objects.Select(o => new KeyVersion { Key = o }).ToList()
            };
            await _client.DeleteObjectsAsync(multiObjectDeleteRequest).ConfigureAwait(false);
        }

        public override async Task<BatchDeleteProcessor> RemovePrefixAsync(string bucketName, string prefix, int chunkSize, CancellationToken cancellationToken = default)
        {
            ValidateInstance();

            var processor = new BatchDeleteProcessor(async (IEnumerable<string> keys) =>
            {
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = bucketName,
                    Objects = keys.Select(k => new KeyVersion { Key = k }).ToList()
                };
                await _client.DeleteObjectsAsync(deleteRequest, cancellationToken).ConfigureAwait(false);
            });

            ListObjectsV2Response response;
            var prefixToFilter = (prefix.EndsWith("/") ? prefix : prefix + "/");
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = prefixToFilter,
                MaxKeys = chunkSize
            };
            do
            {
                response = await _client.ListObjectsV2Async(request);
                if (!response.S3Objects.Any()) continue;

                processor.EnqueueChunk(response.S3Objects.Select(o => o.Key));
                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated);

            return processor;
        }

        ///// <summary>
        ///// Valida se um objeto existe ou não no balde.
        ///// NOTA: Este método é mais rápido segundo informações, porém durante testes, a validação de
        /////       um objeto recém criado apresentava resultado FALSE.
        ///// </summary>
        ///// <param name="bucketName">Nome do balde</param>
        ///// <param name="objectName">Nome do objeto</param>
        ///// <returns>True se existe e False se não existe.</returns>
        //public async Task<bool> ObjectExistByMetadataAsync(string bucketName, string objectName)
        //{
        //    ValidateInstance();
        //    try
        //    {
        //        var metadata = await _client.GetObjectMetadataAsync(bucketName, objectName).ConfigureAwait(false);
        //        if (metadata.HttpStatusCode == System.Net.HttpStatusCode.Found) return true;
        //        return false;
        //    }
        //    catch (AmazonS3Exception ex)
        //    {
        //        if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        //            return false;

        //        //status wasn't not found, so throw the exception
        //        throw;
        //    }
        //}

        /// <summary>
        /// Valida se um objeto existe ou não no balde.
        /// </summary>
        /// <param name="bucketName">Nome do balde</param>
        /// <param name="objectName">Nome do objeto</param>
        /// <returns>True se existe e False se não existe.</returns>
        public override async Task<bool> ObjectExistAsync(string bucketName, string objectName)
        {
            var request = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = objectName,
                MaxKeys = 1
            };

            var response = await _client.ListObjectsV2Async(request).ConfigureAwait(false);

            if (response.S3Objects.Count == 0)
                return false;

            return true;
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
            var result = await _client.GetObjectAsync(bucketName, objectName).ConfigureAwait(false);
            action(result.ResponseStream);
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
            await _client.CopyObjectAsync(bucketName, objectName, destBucketName, destObjectName).ConfigureAwait(false);
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

            return await Task.Run(() =>
            {
                var request = new GetPreSignedUrlRequest
                {
                    BucketName = bucketName,
                    Key = objectName,
                    Verb = HttpVerb.GET,
                    Expires = DateTime.UtcNow.AddSeconds(expiresInt)
                };
                if (reqParams != null)
                {
                    if (reqParams.ContainsKey("response-expires")) request.ResponseHeaderOverrides.Expires = reqParams["response-expires"];
                    if (reqParams.ContainsKey("response-content-type")) request.ResponseHeaderOverrides.ContentType = reqParams["response-content-type"];
                    if (reqParams.ContainsKey("response-cache-control")) request.ResponseHeaderOverrides.CacheControl = reqParams["response-cache-control"];
                    if (reqParams.ContainsKey("response-content-disposition")) request.ResponseHeaderOverrides.ContentDisposition = reqParams["response-content-disposition"];
                }
                return _client.GetPreSignedURL(request);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Efetua a validação se o usuário efetuou a conexão antes de executar as ações.
        /// </summary>
        private void ValidateInstance()
        {
            if (_client == null)
                throw new ObjectDisposedException("Não foi efetuada conexão com o servidor. Utilize a função Connect() antes de chamar as ações.");
        }
    }
}
