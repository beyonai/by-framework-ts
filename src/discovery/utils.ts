import * as os from 'os';
import * as net from 'net';

/**
 * Get local outbound IP address.
 *
 * Detects the local network interface IP by connecting to a target host.
 * If targetHost is localhost/127.0.0.1, uses public IP (8.8.8.8) to probe.
 */
export function getLocalIp(targetHost: string = '8.8.8.8', targetPort: number = 80): string {
    const isLoopback = targetHost === '127.0.0.1' || targetHost === 'localhost' || targetHost === '::1';
    const actualTargetHost = isLoopback ? '8.8.8.8' : targetHost;
    const actualTargetPort = isLoopback ? 80 : targetPort;

    try {
        const sock = net.createConnection({ host: actualTargetHost, port: actualTargetPort });
        const localAddr = sock.localAddress;
        sock.destroy();
        return localAddr ?? '127.0.0.1';
    } catch {
        // Fallback: try to get from network interfaces
        const interfaces = os.networkInterfaces();
        for (const addrs of Object.values(interfaces)) {
            if (!addrs) continue;
            for (const addr of addrs) {
                if (addr && !addr.internal && addr.family === 'IPv4') {
                    return addr.address;
                }
            }
        }
        return '127.0.0.1';
    }
}

/**
 * Synchronous version of getLocalIp using socket connection.
 * Falls back to parsing network interfaces or hostname resolution.
 */
export function getLocalIpSync(targetHost: string = '8.8.8.8', targetPort: number = 80): string {
    const isLoopback = targetHost === '127.0.0.1' || targetHost === 'localhost' || targetHost === '::1';
    const actualTargetHost = isLoopback ? '8.8.8.8' : targetHost;
    const actualTargetPort = isLoopback ? 80 : targetPort;

    try {
        const sock = net.createConnection({ host: actualTargetHost, port: actualTargetPort });
        const localAddr = sock.localAddress;
        sock.destroy();
        return localAddr ?? '127.0.0.1';
    } catch {
        // Fallback: try to get from network interfaces
        const interfaces = os.networkInterfaces();
        for (const addrs of Object.values(interfaces)) {
            if (!addrs) continue;
            for (const addr of addrs) {
                if (addr && !addr.internal && addr.family === 'IPv4') {
                    return addr.address;
                }
            }
        }
        return '127.0.0.1';
    }
}