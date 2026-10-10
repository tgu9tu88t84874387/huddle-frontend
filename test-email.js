require('dotenv').config();
const nodemailer = require('nodemailer');

const t = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT),
  secure: process.env.SMTP_SECURE === 'true',
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS
  }
});

t.verify((err) => {
  if (err) { console.error('❌', err.message); return; }
  console.log('✅ SMTP работает');
  t.sendMail({
    from: `"${process.env.SMTP_FROM_NAME}" <${process.env.SMTP_FROM}>`,
    to: process.env.SMTP_USER,
    subject: 'Huddle SMTP Test',
    text: 'Тестовое письмо от Huddle.'
  }, (e, info) => {
    if (e) console.error('❌ Отправка:', e.message);
    else console.log('📧 Отправлено:', info.messageId);
  });
});