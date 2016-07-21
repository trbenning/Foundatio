namespace Foundatio.RabbitMQ.Queues
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Extensions;
    using Foundatio.Queues;
    using global::RabbitMQ.Client;
    using Logging;
    using Messaging;
    using Nito.AsyncEx;
    using Serializer;

    public class RabbitMQMessageBusQueue<T> : QueueBase<T> where T : class
    {
        private bool _durable;
        private bool _persistent;
        private bool _exclusive;
        private bool _autoDelete;
        private IDictionary<string, object> _queueArguments;
        private TimeSpan _defaultMessageTimeToLive;
        private ConnectionFactory _factory;
        private IConnection _publisherClient;
        private IModel _publisherChannel;
        private string _exchangeName;
        private string _routingKey;
        private IConnection _subscriberClient;
        private IModel _subscriberChannel;
        private readonly AsyncLock _lock = new AsyncLock();
        private long _enqueuedCount;
        private long _dequeuedCount;

        public RabbitMQMessageBusQueue(string userName, string password, string queueName, string routingKey,
            string exhangeName, bool durable, bool persistent,
            bool exclusive, bool autoDelete, IDictionary<string, object> queueArguments = null,
            TimeSpan? defaultMessageTimeToLive = null, ISerializer serializer = null,
            IEnumerable<IQueueBehavior<T>> behaviors = null, ILoggerFactory loggerFactory = null)
            : base(serializer, behaviors, loggerFactory)
        {
            _serializer = serializer ?? new JsonNetSerializer();
            _queueName = queueName;
            _exchangeName = exhangeName;
            _routingKey = routingKey;
            _durable = durable;
            _persistent = persistent;
            _exclusive = exclusive;
            _autoDelete = autoDelete;
            _queueArguments = queueArguments;

            if (defaultMessageTimeToLive.HasValue && defaultMessageTimeToLive.Value > TimeSpan.Zero)
                _defaultMessageTimeToLive = defaultMessageTimeToLive.Value;

            // initialize connection factory
            _factory = new ConnectionFactory
            {
                UserName = userName,
                Password = password
            };

            // initialize publisher
            _publisherClient = _factory.CreateConnection();
            _publisherChannel = _publisherClient.CreateModel();
            SetUpExchangeAndQueuesForRouting(_publisherChannel);
            _logger.Trace("The unique channel number for the publisher is : {channelNumber}",
                _publisherChannel.ChannelNumber);

            // initialize subscriber
            _subscriberClient = _factory.CreateConnection();
            _subscriberChannel = _subscriberClient.CreateModel();
            SetUpExchangeAndQueuesForRouting(_subscriberChannel);
            _logger.Trace("The unique channel number for the subscriber is : {channelNumber}",
                _subscriberChannel.ChannelNumber);
        }

        protected override Task EnsureQueueCreatedAsync(CancellationToken cancellationToken = new CancellationToken())
            => Task.CompletedTask;

        protected override async Task<string> EnqueueImplAsync(T data)
        {
            if (!await OnEnqueuingAsync(data).AnyContext())
                return null;

            Interlocked.Increment(ref _enqueuedCount);

            if (data == null)
                return string.Empty;

            _logger.Trace("Message Publish: {messageType}", typeof(T).FullName);

            var messageBytes = await _serializer.SerializeAsync(new MessageBusData
            {
                Type = typeof(T).AssemblyQualifiedName,
                Data = await _serializer.SerializeToStringAsync(data).AnyContext()
            }).AnyContext();

            var basicProperties = _publisherChannel.CreateBasicProperties();
            basicProperties.Persistent = _persistent;
            basicProperties.Expiration = _defaultMessageTimeToLive.Milliseconds.ToString();

            // The publication occurs with mandatory=false
            _publisherChannel.BasicPublish(_exchangeName, _routingKey, basicProperties, messageBytes);
            return basicProperties.MessageId;
        }

        protected override async Task<IQueueEntry<T>> DequeueImplAsync(CancellationToken cancellationToken)
        {
            await EnsureQueueCreatedAsync(cancellationToken).AnyContext();

            var get = _subscriberChannel.BasicGet(_queueName, false);
            if (get.Body == null)
                return null;

            var item = await _serializer.DeserializeAsync<T>(get.Body).AnyContext();
            Interlocked.Increment(ref _dequeuedCount);
            var entry = new QueueEntry<T>(
                get.BasicProperties.MessageId, 
                item, 
                this, 
                DateTimeOffset.FromUnixTimeMilliseconds(get.BasicProperties.Timestamp.UnixTime).DateTime, 
                Convert.ToInt32(get.DeliveryTag));

            await OnDequeuedAsync(entry).AnyContext();
            return entry;
        }

        public override Task RenewLockAsync(IQueueEntry<T> queueEntry)
        {
            throw new NotImplementedException();
        }

        public override Task CompleteAsync(IQueueEntry<T> queueEntry)
        {
            throw new NotImplementedException();
        }

        public override Task AbandonAsync(IQueueEntry<T> queueEntry)
        {
            throw new NotImplementedException();
        }

        protected override Task<IEnumerable<T>> GetDeadletterItemsImplAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<QueueStats> GetQueueStatsImplAsync()
        {
            throw new NotImplementedException();
        }

        public override Task DeleteQueueAsync()
        {
            throw new NotImplementedException();
        }

        protected override void StartWorkingImpl(Func<IQueueEntry<T>, CancellationToken, Task> handler,
            bool autoComplete, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private void SetUpExchangeAndQueuesForRouting(IModel model)
        {
            // setup the message router - it requires the name of our exchange, exhange type and durability
            // ( it will survive a server restart )
            model.ExchangeDeclare(_exchangeName, ExchangeType.Direct, _durable);
            // setup the queue where the messages will reside - it requires the queue name and durability.
            model.QueueDeclare(_queueName, _durable, _exclusive, _autoDelete, _queueArguments);
            // bind the queue with the exchange.
            model.QueueBind(_queueName, _exchangeName, _routingKey);
        }

        public override void Dispose()
        {
            base.Dispose();
            CloseConnection();
        }

        private void CloseConnection()
        {
            if (_subscriberChannel != null && _subscriberChannel.IsOpen)
                _subscriberChannel.Close();
            _subscriberChannel?.Dispose();

            if (_subscriberClient != null && _subscriberClient.IsOpen)
                _subscriberClient.Close();
            _subscriberClient?.Dispose();

            if (_publisherChannel != null && _publisherChannel.IsOpen)
                _publisherChannel.Close();
            _publisherChannel?.Dispose();

            if (_publisherClient != null && _publisherClient.IsOpen)
                _publisherClient.Close();
            _publisherClient?.Dispose();
        }
    }
}
