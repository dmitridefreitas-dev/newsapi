import dotenv from 'dotenv';
dotenv.config();
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import { ProxyAgent, setGlobalDispatcher } from 'undici';

import routes from './routes/index.js';
import { errorMiddleware } from './middleware/index.js';
import logger from './utils/logger.js';


// Route all outbound fetch() (incl. Yahoo Finance) through a residential proxy.
// Datacenter IPs (Render, etc.) get 429 "Failed to get crumb" from Yahoo; a
// residential proxy bypasses that. Set PROXY_URL on Render to e.g.
// http://user:pass@host:port. Without it, requests go direct (current behaviour).
if (process.env.PROXY_URL) {
	setGlobalDispatcher(new ProxyAgent(process.env.PROXY_URL));
	logger.info('🌐 Outbound HTTP routed through PROXY_URL');
} else {
	logger.warn('PROXY_URL not set — outbound calls go direct (Yahoo may 429 on cloud IPs)');
}

const app = express();
app.set('etag', false);

process.on('uncaughtException', (error) => {
	logger.error('Uncaught exception:', error);
});
  
process.on('unhandledRejection', (reason, promise) => {
	logger.error('Unhandled rejection at:', promise, 'reason:', reason);
});

process.on('SIGINT', async () => {
	logger.info('Interrupted');
	process.exit(0);
});

process.on('SIGTERM', async () => {
	logger.info('SIGTERM signal received');

	await new Promise(resolve => setTimeout(resolve, 3000));

	logger.info('Exiting');
	process.exit();
});

app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ extended: true, limit: '15mb' }));

const apiRoutes = routes();
app.use('/', apiRoutes);
app.use('/hcgi/api', apiRoutes);

app.use(errorMiddleware);

app.use((req, res) => {
	res.status(404).json({ error: 'Route not found' });
});

const port = process.env.PORT || 3001;

app.listen(port, () => {
	logger.info(`🚀 API Server running on http://localhost:${port}`);
});

export default app;