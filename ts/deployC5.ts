import fs from 'fs/promises';
import { Cell, toNano } from '@ton/core';
import { NetworkProvider } from '@ton/blueprint';
import { LoggerNew } from './Logger';
import { execSync } from 'child_process';
import { getSecureRandomBytes, keyPairFromSeed } from '@ton/crypto';

export async function run(provider: NetworkProvider) {
    const ui = provider.ui();

    const keypair = keyPairFromSeed(await getSecureRandomBytes(32));
    const seed = keypair.secretKey.toString('hex').slice(0, 64);
    provider.ui().write(`seed: ${seed}`);
    await fs.writeFile('seed.hex', seed);

    const pubkey = '0x' + keypair.publicKey.toString('hex');
    ui.write(`pubkey: ${pubkey}`);

    const hexcode = execSync(`./contracts/logger-c5.fif 0 ${pubkey}`).toString();
    ui.write(`code: ${hexcode}`);

    const code = Cell.fromBoc(Buffer.from(hexcode, 'hex'))[0];

    const logger = provider.open(LoggerNew.createFromConfig({ seqno: Cell.EMPTY }, code));

    ui.write(`${logger.address} ${seed}`);

    await logger.sendDeploy(provider.sender(), toNano('0.1'));

    await provider.waitForDeploy(logger.address);
}
