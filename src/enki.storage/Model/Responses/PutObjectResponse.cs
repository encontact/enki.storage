using enki.storage.Interface.Responses;

namespace enki.storage.Model
{
	public class PutObjectResponse : IPutObjectResponse
	{
		public bool SuccessResult { get; private set; }

		public PutObjectResponse(bool result) => SuccessResult = result;
	}
}