using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Enyim.Caching.Tests
{
	public class MemcachedClientRemoveTests : MemcachedClientTestsBase
	{
		[Fact]
		public void When_Removing_A_Valid_Key_Result_Is_Successful()
		{
			var key = GetUniqueKey("remove");
			var storeResult = Store(key: key);
			StoreAssertPass(storeResult);

			var removeResult = _client.ExecuteRemove(key);
			Assert.True(removeResult.Success, "Success was false");
			Assert.True((removeResult.StatusCode ?? 0) == 0, "StatusCode was neither null nor 0");

			var getResult = _client.ExecuteGet(key);
			GetAssertFail(getResult);
		}

		[Fact]
		public void When_Removing_An_Invalid_Key_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("remove");

			var removeResult = _client.ExecuteRemove(key);
			Assert.False(removeResult.Success, "Success was true");
		}

		[Fact]
		public void When_Removing_Multiple_Keys_Present_Keys_Are_Removed()
		{
			var keys = GetUniqueKeys("remove_multi", 5).ToList();
			foreach (var key in keys)
			{
				StoreAssertPass(Store(key: key));
			}

			Assert.True(_client.Remove(keys));

			foreach (var key in keys)
			{
				GetAssertFail(_client.ExecuteGet(key));
			}
		}

		[Fact]
		public void When_First_Key_Is_Missing_Remaining_Keys_Are_Still_Removed()
		{
			var keys = GetUniqueKeys("remove_multi_first_missing", 5).ToList();
			for (int i = 1; i < keys.Count; i++)
			{
				StoreAssertPass(Store(key: keys[i]));
			}

			Assert.True(_client.Remove(keys));

			for (int i = 1; i < keys.Count; i++)
			{
				GetAssertFail(_client.ExecuteGet(keys[i]));
			}
		}

		[Fact]
		public void When_Removing_Empty_Key_List_Result_Is_Successful()
		{
			Assert.True(_client.Remove(Enumerable.Empty<string>()));
		}
	}
}
