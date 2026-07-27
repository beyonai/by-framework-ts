# Deploy smoke test image: runs examples/demo_worker.ts (a plain echo worker)
# against a real Redis instance via ts-node — no separate compile step needed,
# so this stays a single stage. Driven by deploy/docker-compose.yml and
# .github/workflows/deploy-smoke-test.yml.

FROM node:20-alpine
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src src
COPY examples examples

# Overridable so the same image can also run send_and_verify.ts if needed.
ENV WORKER_SCRIPT=examples/demo_worker.ts
ENTRYPOINT ["sh", "-c", "npx ts-node \"$WORKER_SCRIPT\""]
