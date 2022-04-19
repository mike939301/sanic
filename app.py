import asyncio
import json
import logging

import aiormq
import aiormq.types

from app.db import m_db
from engine.logging.models import Log
from engine.moderators.models import ModerationTask
from engine.comments.cmt_rabbitmq.config import config


logger = logging.getLogger(__name__)


async def consume_cmt(connection: aiormq.Connection):
    # Creating a channel
    channel = await connection.channel()
    await channel.basic_qos(prefetch_count=1)

    await channel.exchange_declare(
        exchange=config.get('exchange_name'), exchange_type='topic', durable=True,
    )

    # Declaring queue
    declare_ok = await channel.queue_declare(config.get('queue_name'), durable=True)

    # Binding the queue to the exchange
    await channel.queue_bind(declare_ok.queue, config.get('exchange_name'), config.get('routing_key'))

    # Start listening the queue with name 'task_queue'
    await channel.basic_consume(declare_ok.queue, on_message)


async def on_message(message: aiormq.types.DeliveredMessage):

    try:
        body_json = json.loads(message.body)
        await handler(body_json)
    except asyncio.CancelledError as e:
        logger.exception(f'{e}')
        await message.channel.basic_nack(message.delivery.delivery_tag)
        return
    except Exception:
        logger.exception('Error handling message')

    await message.channel.basic_ack(message.delivery.delivery_tag)


async def handler(comment: dict):
    """:comment - {'comment_id': 1601855787102779, 'type': 'comment', 'text': 'text1'}
                  {'comment_id': 1386253622648009, 'type': 'message', 'text': 'text2'}
        """

    tasks = await ModerationTask.query.where((ModerationTask.comment_id == comment.get('comment_id')) & (
            ModerationTask.comment_state == comment.get('text'))).gino.all()

    if tasks:
        return

    log_query = m_db.select([m_db.func.count(Log.id)]).where((Log.comment_id == comment.get('comment_id')) &
                                                             (Log.comment_state == comment.get('text')))

    sent_logs_cnt = await log_query.where((Log.action == Log.ACTION_SENT)).gino.scalar()

    if sent_logs_cnt == 0:
        # Not sent yet - create brand new moderation task
        status = ModerationTask.STATUS_NEW
    else:
        status = ModerationTask.STATUS_WAIT

    await ModerationTask.create(status=status, type=comment.get('type'),
                                comment_id=comment.get('comment_id'),
                                comment_state=comment.get('text'))

