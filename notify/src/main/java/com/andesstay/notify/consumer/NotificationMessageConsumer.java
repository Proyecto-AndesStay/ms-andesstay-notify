package com.andesstay.notify.consumer;

import com.andesstay.notify.dto.MessageEnvelope;
import com.andesstay.notify.dto.NotificationPayload;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class NotificationMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationMessageConsumer.class);

    // 1. Consumidor de Email / Push al Huésped (q.cmd.email)
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}")
    public void processEmailNotification(@Payload MessageEnvelope<NotificationPayload> envelope,
                                         Channel channel,
                                         @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            log.info("--> [CONSUMER EMAIL] Recibido evento: {} | Correlacion: {}", envelope.getEventId(), envelope.getCorrelationId());
            NotificationPayload data = envelope.getPayload();

            if (data == null || data.getGuestEmail() == null || data.getGuestEmail().isBlank()) {
                throw new IllegalArgumentException("Payload inválido: falta correo del destinatario");
            }

            log.info("Simulando envío de correo a: {} | Asunto/Mensaje: {}", data.getGuestEmail(), data.getMessage());

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("<-- [CONSUMER EMAIL] Procesado con éxito. ACK tag: {}", deliveryTag);

        } catch (Exception e) {
            log.error("Fallo al procesar correo: {}. Enviando a DLQ...", e.getMessage());
            // Rechazo explícito con requeue=false para enviar automáticamente a Dead Letter Queue (DLQ)
            channel.basicNack(deliveryTag, false, false);
        }
    }

    // 2. Consumidor de Ticket Housekeeping / Limpieza (q.cmd.housekeeping)
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}")
    public void processHousekeepingTicket(@Payload MessageEnvelope<NotificationPayload> envelope,
                                          Channel channel,
                                          @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            log.info("--> [CONSUMER HOUSEKEEPING] Generando ticket de limpieza para unidad: {}", envelope.getPayload().getUnitId());

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("<-- [CONSUMER HOUSEKEEPING] Ticket completado. ACK tag: {}", deliveryTag);

        } catch (Exception e) {
            log.error("Fallo en ticket de limpieza: {}. Enviando a DLQ...", e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }

    // 3. Consumidor de Generación de Voucher / PDF (q.cmd.voucher)
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}")
    public void processVoucherGeneration(@Payload MessageEnvelope<NotificationPayload> envelope,
                                         Channel channel,
                                         @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            log.info("--> [CONSUMER VOUCHER] Generando voucher PDF para reserva: {}", envelope.getPayload().getReservationId());

            // Confirmación explícita (ACK)
            channel.basicAck(deliveryTag, false);
            log.info("<-- [CONSUMER VOUCHER] Voucher generado. ACK tag: {}", deliveryTag);

        } catch (Exception e) {
            log.error("Fallo al generar voucher: {}. Enviando a DLQ...", e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}