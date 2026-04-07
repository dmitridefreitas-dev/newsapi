import { Router } from 'express';
import healthCheck from './health-check.js';
import contactRouter from './contact.js';
import marketDataRouter from './market-data.js';
import newsRouter from './news.js';
import tickerRouter from './ticker.js';

const router = Router();

export default () => {
    router.get('/health', healthCheck);
    router.use('/contact', contactRouter);
    router.use('/market-data', marketDataRouter);
    router.use('/news', newsRouter);
    router.use('/ticker', tickerRouter);

    return router;
};