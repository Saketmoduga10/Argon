# Argon dashboard + API in one image.
# Stage 1 builds the React dashboard; stage 2 serves it with the API.
FROM node:22-alpine AS dashboard
WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY argon/ ./argon/
COPY benchmarks/ ./benchmarks/
COPY --from=dashboard /app/dashboard/dist ./dashboard/dist
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "argon.server:app", "--host", "0.0.0.0", "--port", "8000"]
