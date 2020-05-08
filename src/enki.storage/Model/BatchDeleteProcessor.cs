using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace enki.storage.Model
{
    public class BatchDeleteProcessor
    {
        private readonly Func<IEnumerable<string>, Task> deleteObjectsAction;
        private readonly IList<Task> deleteTasks;

        public BatchDeleteProcessor(Func<IEnumerable<string>, Task> deleteAction)
        {
            deleteObjectsAction = deleteAction;
            deleteTasks = new List<Task>();
        }

        public void EnqueueChunk(IEnumerable<string> keys)
        {
            deleteTasks.Add(DeleteChunk(keys));
        }

        public void WaitComplete()
        {
            Task.WaitAll(deleteTasks.ToArray());
        }

        private async Task DeleteChunk(IEnumerable<string> keys)
        {
            await deleteObjectsAction(keys).ConfigureAwait(false);
        }
    }
}
