import { Hono } from "hono";
import { Resend } from "resend";

interface QueueMessage {
  userId: number;
  message?: string;
  email?: string;
  time?: string;
}

const app = new Hono<Env>();

app.get("/", (c) => {
  return c.text("Avoid Resend Rate Limits!");
});

// POST with body QueueMessage

app.post("/api/email", async (c) => {
  const { userId } = await c.req.json();

  try {
    await c.env.EMAIL_QUEUE.send(
      {
        userId: userId,
        // Some other message content
        message: "Hello Resend",

        /* 
        // Email address to send to. Alternatively, fetch it from database in consumer
        email: "delivered@example.com",
        */
      },
      // Message is sent with a delay to avoid rate-limiting
      { delaySeconds: 1 }
    );
    return c.json({ message: "Message sent", status: 200 });
  } catch (e) {
    console.error(e);
    return c.json({
      message: "Failed to send message to the queue",
      status: 500,
    });
  }
});

export default {
  fetch: app.fetch,
  queue: async (batch: MessageBatch<QueueMessage>, env: Env) => {
    // Handle multiple queues. Add more cases for other queues
    switch (batch.queue) {
      case "resend-demo":
        await handleSendEmail(batch, env);
        break;
      default:
        console.error("Unknown Queue");
    }
  },
};

const handleSendEmail = async (batch: MessageBatch<QueueMessage>, env: Env) => {
  console.log("Handling Send Email event");

  const resend = new Resend(env.RESEND_API_KEY);

  for (let queueMessage of batch.messages) {
    try {
      const { body } = queueMessage;

      // Fetch email from the database if not provided in the message

      console.log("Sending email to", body.email);
      const sendMessage = await resend.emails.send({
        from: "onboarding@resend.dev",
        to: [body.email || "delivered@resend.dev"],
        subject: "Hello World",
        html: `<p>Hi ${body.userId}, ${body.message}</p>`,
      });

      // Check if it was a success
      if (sendMessage.error === null) {
        // Ack the message on success
        queueMessage.ack();
        console.log("Message sent successfully");
      } else {
        // Log the error and retry with a delay of 5s
        console.error(sendMessage.error.message);
        queueMessage.retry({ delaySeconds: 5 });
      }
    } catch (e) {
      console.error("An error occured", e);
      queueMessage.retry({ delaySeconds: 5 });
    }
  }
};

// For stress testing, we'll use a simple endpoint to send a bunch of messages to the queue

/*
app.get("/api/stress-test-limits", async (c) => {
  const iterations = c.req.query("iterations")
    ? parseInt(c.req.query("iterations"))
    : 30;

  const results = [];

  for (let i = 0; i < iterations; i++) {
    const startTime = Date.now();

    // Alternate between '@resend.dev' and 'example.com'
    const email = i % 3 === 0 ? "delivered@resend.dev" : "delivered@example.com";

    const message: QueueMessage = {
      userId: i,
      time: new Date().toISOString(),
      email: email,
    };

    try {
      await c.env.EMAIL_QUEUE.send(message, { delaySeconds: 1 });
      const endTime = Date.now();
      results.push({
        iteration: i + 1,
        duration: endTime - startTime,
        time: new Date(parseInt(message.time)).toLocaleString(),
        email: email,
      });
      console.log(`Message sent for id ${i}`);
    } catch (e) {
      console.error(`Failed to send message for iteration ${i + 1}:`, e);
      results.push({
        iteration: i + 1,
        error: "Failed to send message",
        email: email,
      });
    }
  }

  return c.json({
    totalIterations: iterations,
    successfulIterations: results.filter((r) => !r.error).length,
    averageDuration:
      results.reduce((sum, r) => (r.duration ? sum + r.duration : sum), 0) /
      results.filter((r) => r.duration).length,
    results: results,
  });
});
*/
