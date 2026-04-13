import { Router } from 'express';
import healthCheck from './health-check.js';
import contactRouter from './contact.js';
import marketDataRouter from './market-data.js';
import newsRouter from './news.js';
import tickerRouter from './ticker.js';
import chatRouter from './chat.js';
import transcribeRouter from './transcribe.js';
import ttsRouter from './tts.js';

const router = Router();

export default () => {
    router.get('/health', healthCheck);
    router.use('/contact', contactRouter);
    router.use('/market-data', marketDataRouter);
    router.use('/news', newsRouter);
    router.use('/ticker', tickerRouter);
    router.use('/chat', chatRouter);
    router.use('/transcribe', transcribeRouter);
    router.use('/tts', ttsRouter);

    return router;
};