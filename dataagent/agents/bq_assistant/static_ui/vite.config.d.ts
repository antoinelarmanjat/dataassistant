declare const _default: () => Promise<{
    plugins: import("vite").Plugin<any>[];
    build: {
        rollupOptions: {
            input: Record<string, string>;
        };
        target: string;
    };
    define: {};
    resolve: {
        dedupe: string[];
        alias: {
            "@a2ui/markdown-it": string;
            "sandbox.js": string;
            "@modelcontextprotocol/ext-apps/app-bridge": string;
        };
    };
    optimizeDeps: {
        esbuildOptions: {
            target: string;
        };
    };
    server: {
        host: true;
    };
}>;
export default _default;
//# sourceMappingURL=vite.config.d.ts.map