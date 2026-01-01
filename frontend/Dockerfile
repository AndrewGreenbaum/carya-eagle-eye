FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --legacy-peer-deps
COPY . .

# Set production API URL for Vite build
ENV VITE_API_BASE=https://bud-tracker-backend-production.up.railway.app
ENV VITE_API_KEY=dev-key

RUN npm run build
RUN npm install -g serve
ENV PORT=3000
EXPOSE 3000
CMD ["sh", "-c", "serve -s dist -l ${PORT}"]
