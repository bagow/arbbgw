/**
 * index.js – Cloudflare Worker (Static Assets & Lightweight Redirector)
 * 
 * All API processing, WebSocket streams, and kline fetching have been offloaded
 * to the VPS backend (vps-api via Cloudflare Tunnel at api.arb.bagow.com).
 * This worker serves static assets from public/ and redirects any fallback /api/* calls.
 */

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        // Instant redirect for any API calls to direct VPS Cloudflare Tunnel
        if (url.pathname.startsWith('/api/')) {
            const redirectUrl = new URL(request.url);
            redirectUrl.hostname = 'api.arb.bagow.com';
            redirectUrl.protocol = 'https:';
            redirectUrl.port = '';
            return Response.redirect(redirectUrl.toString(), 307);
        }

        // Serve static dashboard assets from ./public
        if (env.ASSETS) {
            return await env.ASSETS.fetch(request);
        }

        return new Response('Not Found', { status: 404 });
    },
};
