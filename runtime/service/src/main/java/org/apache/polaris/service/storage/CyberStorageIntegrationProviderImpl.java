package org.apache.polaris.service.storage;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegration.ValidationResult;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.azure.AzureCredentialsStorageIntegration;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpCredentialsStorageIntegration;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@Alternative
@Priority(10)
public class CyberStorageIntegrationProviderImpl implements PolarisStorageIntegrationProvider {
    private final StsClientProvider stsClientProvider;
  private final Optional<AwsCredentialsProvider> stsCredentials;
  private final Supplier<GoogleCredentials> gcpCredsProvider;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public CyberStorageIntegrationProviderImpl(
      StorageConfiguration storageConfiguration, StsClientProvider stsClientProvider, Clock clock) {
    this(
        stsClientProvider,
        Optional.ofNullable(storageConfiguration.stsCredentials()),
        storageConfiguration.gcpCredentialsSupplier(clock));
  }

  public CyberStorageIntegrationProviderImpl(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      Supplier<GoogleCredentials> gcpCredsProvider) {
    this.stsClientProvider = stsClientProvider;
    this.stsCredentials = stsCredentials;
    this.gcpCredsProvider = gcpCredsProvider;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends PolarisStorageConfigurationInfo>
      @Nullable PolarisStorageIntegration<T> getStorageIntegrationForConfig(
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    if (polarisStorageConfigurationInfo == null) {
      return null;
    }
    PolarisStorageIntegration<T> storageIntegration;
    switch (polarisStorageConfigurationInfo.getStorageType()) {
      case S3:
        storageIntegration =
            (PolarisStorageIntegration<T>)
                new CyberS3CredentialsStorageIntegration(
                    (AwsStorageConfigurationInfo) polarisStorageConfigurationInfo,
                    stsClientProvider,
                    stsCredentials);
        break;
      case GCS:
        storageIntegration =
            (PolarisStorageIntegration<T>)
                new GcpCredentialsStorageIntegration(
                    (GcpStorageConfigurationInfo) polarisStorageConfigurationInfo,
                    gcpCredsProvider.get(),
                    ServiceOptions.getFromServiceLoader(
                        HttpTransportFactory.class, NetHttpTransport::new));
        break;
      case AZURE:
        storageIntegration =
            (PolarisStorageIntegration<T>)
                new AzureCredentialsStorageIntegration(
                    (AzureStorageConfigurationInfo) polarisStorageConfigurationInfo);
        break;
      case FILE:
        storageIntegration =
            new PolarisStorageIntegration<>((T) polarisStorageConfigurationInfo, "file") {
              @Override
              public StorageAccessConfig getSubscopedCreds(
                  @Nonnull RealmConfig realmConfig,
                  boolean allowListOperation,
                  @Nonnull Set<String> allowedReadLocations,
                  @Nonnull Set<String> allowedWriteLocations,
                  Optional<String> refreshCredentialsEndpoint) {
                return StorageAccessConfig.builder().supportsCredentialVending(false).build();
              }

              @Override
              public @Nonnull Map<String, Map<PolarisStorageActions, ValidationResult>>
                  validateAccessToLocations(
                      @Nonnull RealmConfig realmConfig,
                      @Nonnull T storageConfig,
                      @Nonnull Set<PolarisStorageActions> actions,
                      @Nonnull Set<String> locations) {
                return Map.of();
              }
            };
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown storage type " + polarisStorageConfigurationInfo.getStorageType());
    }
    return storageIntegration;
  }
}
