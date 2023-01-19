using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RVShershnev.YandexCloud.DataStreams
{
    public class DataStreamsConfiguration
    {
        public string KeyId { get; set; }
        public string Secret { get; set; }
        public string ServiceURL { get; set; }

        /// <summary>
        /// Название региона.
        /// </summary>
        public string Region { get; set; }

        /// <summary>
        /// Название каталога.
        /// </summary>
        public string Folder { get; set; }

        /// <summary>
        /// Название базы данных.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Название потока данных.  
        /// </summary>
        public string StreamName { get; set; }

        /// <summary>
        /// Путь к потоку данных.
        /// </summary>
        public string StreamPath { get => $"/{Region}/{Folder}/{Database}/{StreamName}"; }

        /// <summary>
        /// Ключ сегмента.
        /// </summary>
        public string PartitionKey { set; get; } = "1";
    }
}
