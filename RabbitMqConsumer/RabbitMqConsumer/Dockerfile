
FROM mcr.microsoft.com/dotnet/core/aspnet:3.1 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build

# COPY bin/Release/netcoreapp3.1/publish/ App/
WORKDIR /src
COPY ["RabbitMqConsumer.csproj", "RabbitMqConsumer/"]
COPY . RabbitMqConsumer/
WORKDIR "/src/RabbitMqConsumer"
RUN dotnet build "RabbitMqConsumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "RabbitMqConsumer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "RabbitMqConsumer.dll"]
# ENTRYPOINT ["dotnet", "RabbitMqConsumer.dll"]