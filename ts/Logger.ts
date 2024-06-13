import {
    Address,
    beginCell,
    Cell,
    Contract,
    contractAddress,
    ContractProvider,
    external,
    Sender,
    SendMode,
    storeMessage,
    toNano,
} from '@ton/core';

import { KeyPair, sign } from '@ton/crypto';

export type LoggerNewConfig = {
    seqno: Cell;
};

export function loggerConfigToCell(config: LoggerNewConfig): Cell {
    return config.seqno;
}

export class LoggerNew implements Contract {
    constructor(
        readonly address: Address,
        readonly config: LoggerNewConfig,
        readonly init?: { code: Cell; data: Cell },
    ) {}

    static createFromAddress(address: Address, config: LoggerNewConfig) {
        return new LoggerNew(address, config);
    }

    static createFromConfig(config: LoggerNewConfig, code: Cell, workchain = 0) {
        const data = loggerConfigToCell(config);
        const init = { code, data };
        return new LoggerNew(contractAddress(workchain, init), config, init);
    }

    async sendDeploy(provider: ContractProvider, via: Sender, value: bigint = toNano('1')) {
        await provider.internal(via, {
            value,
            sendMode: SendMode.PAY_GAS_SEPARATELY,
        });
    }

    async sendExternal(provider: ContractProvider, someValue: number) {
        // let seqno = await this.getSeqno(provider);
        const body = beginCell().storeUint(someValue, 256).endCell();
        const query = beginCell().storeRef(body).endCell();
        await provider.external(query);
    }

    async sendExternalBalance(provider: ContractProvider) {
        const state = await provider.getState();
        const query = beginCell().storeCoins(state.balance).endCell();
        await provider.external(query);
    }

    async sendCreateC5Boc(provider: ContractProvider, actionsList: Cell, seqno?: number): Promise<Cell> {
        if (!seqno) seqno = await this.getIntSeqno(provider);
        const actions = beginCell().storeRef(actionsList).storeUint(seqno, 64).endCell();
        const query = beginCell().storeUint(seqno, 32).storeRef(actions).endCell();
        const msg = external({ to: this.address, body: query });
        let b = beginCell();
        storeMessage(msg)(b);
        return b.endCell();
    }

    async sendCreateC5BocSigned(
        provider: ContractProvider,
        actions: Cell,
        secretKey: Buffer,
        seqno?: number,
    ): Promise<Cell> {
        if (!seqno) seqno = await this.getIntSeqno(provider);

        const body = beginCell()
            .storeUint(seqno, 32)
            .storeUint(Math.floor(Date.now() / 1000) + 60, 48) // valid_until
            .storeRef(actions)
            .endCell();
        const hash = body.hash();
        const signature = sign(hash, secretKey);
        const query = beginCell() // msg_slice for contract
            .storeBuffer(signature, 64)
            .storeSlice(body.asSlice())
            .endCell();

        const msg = external({ to: this.address, body: query });
        let b = beginCell();
        storeMessage(msg)(b);
        return b.endCell();
    }

    async getSeqno(provider: ContractProvider) {
        try {
            const { stack } = await provider.get('seqno', []);
            return stack.readCell().hash();
        } catch (e) {
            return;
        }
    }

    async getIntSeqno(provider: ContractProvider) {
        try {
            const { stack } = await provider.get('seqno', []);
            return stack.readNumber();
        } catch (e) {
            console.log(e);
            return -1;
        }
    }
}
