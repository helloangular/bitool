FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY server.js .
COPY openapi.json .
COPY data.csv .
EXPOSE 3002
ENV PORT=3002
CMD ["node", "server.js"]
