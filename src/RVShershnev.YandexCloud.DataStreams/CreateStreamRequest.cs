using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RVShershnev.YandexCloud.DataStreams
{
    public record CreateStreamRequest
    {
        /// <summary>
        /// Название создаваемого потока данных.
        /// </summary>        
        public string StreamName { get; init; }

        /// <summary>
        /// Количество сегментов в потоке.
        /// </summary>
        public int ShardCount { get; init; }

        /// <summary>
        /// Ограничение на запись в поток данных, КБ/с.
        /// </summary>
        public int WriteQuotaKbPerSec { get; init; }

        /// <summary>
        /// Время хранения сообщений, часы.
        /// </summary>
        public int RetentionPeriodHours { get; init; }
    }
}
