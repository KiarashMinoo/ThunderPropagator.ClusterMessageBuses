using System.Globalization;

namespace ThunderPropagator.ClusterMessageBuses.AwsSqs
{
    /// <summary>
    /// Builds the SQS queue-access IAM policy that grants an SNS topic permission to deliver to a
    /// specific queue — required before <c>SubscribeAsync</c>-ing that queue to that topic, since
    /// SQS queues default to denying every principal except their own account/owner.
    /// </summary>
    internal static class AwsSqsQueuePolicy
    {
        /// <summary>
        /// Builds a minimal IAM policy document allowing the <c>sns.amazonaws.com</c> service
        /// principal to call <c>sqs:SendMessage</c> on <paramref name="queueArn"/>, scoped (via the
        /// <c>aws:SourceArn</c> condition) to deliveries originating specifically from
        /// <paramref name="topicArn"/> — so this grant can never be exercised by any other topic.
        /// </summary>
        internal static string AllowSnsPublish(string queueArn, string topicArn)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                """
                {{
                  "Version": "2012-10-17",
                  "Id": "{0}/SnsPublishPolicy",
                  "Statement": [
                    {{
                      "Sid": "AllowSnsPublish",
                      "Effect": "Allow",
                      "Principal": {{ "Service": "sns.amazonaws.com" }},
                      "Action": "sqs:SendMessage",
                      "Resource": "{0}",
                      "Condition": {{ "ArnEquals": {{ "aws:SourceArn": "{1}" }} }}
                    }}
                  ]
                }}
                """,
                queueArn,
                topicArn);
        }
    }
}
