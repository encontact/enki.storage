using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace enki.storage.Model
{
	public class CreateMD5CheckSum
	{
		private MD5 md5;
		private string Base64CreatedMd5 { get; set; }
		private string CreatedMd5 { get; set; }

		private CreateMD5CheckSum()
			=> md5 = MD5.Create();
		public CreateMD5CheckSum(byte[] file) : this()
		{
			var hash = CalculateMD5Hash(file);
			Base64CreatedMd5 = Convert.ToBase64String(hash);
			CreatedMd5 = PrepareMD5(hash);
		}
		public CreateMD5CheckSum(Stream file) : this()
		{
			var hash = CalculateMD5Hash(file);
			Base64CreatedMd5 = Convert.ToBase64String(hash);
			CreatedMd5 = PrepareMD5(hash);
		}

		public CreateMD5CheckSum(string filePath) : this()
		{
			var hash = CalculateMD5Hash(File.ReadAllBytes(filePath));
			Base64CreatedMd5 = Convert.ToBase64String(hash);
			CreatedMd5 = PrepareMD5(hash);
		}

		public string GetMd5()
			=> CreatedMd5;
		public string GetBase64Md5()
			=> Base64CreatedMd5;

		public bool Validate(string md5ToValidade)
			=> CreatedMd5 == md5ToValidade;

		private byte[] CalculateMD5Hash(byte[] bytes)
			=> md5.ComputeHash(bytes);

		private byte[] CalculateMD5Hash(Stream inputStream)
			=> md5.ComputeHash(inputStream);

		private static string PrepareMD5(byte[] hashBytes)
		{
			var sb = new StringBuilder();
			for (int i = 0; i < hashBytes.Length; i++)
			{
				sb.Append(hashBytes[i].ToString("x2"));
			}
			return sb.ToString();
		}
	}
}
