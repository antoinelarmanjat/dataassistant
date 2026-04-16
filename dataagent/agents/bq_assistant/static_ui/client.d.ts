import { v0_8 } from "@a2ui/lit";
export declare class A2UIClient {
    #private;
    get ready(): Promise<void>;
    setAuthToken(token: string | null): void;
    send(message: v0_8.Types.A2UIClientEventMessage, onChunk?: (messages: v0_8.Types.ServerToClientMessage[]) => void): Promise<v0_8.Types.ServerToClientMessage[]>;
}
//# sourceMappingURL=client.d.ts.map