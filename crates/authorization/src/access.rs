use crate::*;
use std::sync::Arc;

/// Fresh, trusted facts supplied by the host's existing access/session service.
/// This is not a token, a serializable proof, or a substitute for authentication.
pub struct AccessIdentity {
    pub principal: Principal,
    pub audience: String,
    /// Effective session expiry, including the host's idle and absolute limits.
    /// None is for identities without a session, not an unknown session expiry.
    pub session_expires_at: Option<u64>,
    pub credential: Option<CredentialLimit>,
}

/// Bridge to an existing trusted host access service. Every invocation must run
/// its normal verification/revocation/membership chain for this evidence and
/// resource. Return None for refusal, including reauthentication requirements.
/// Errors remain opaque. Never implement this by trusting a public identity DTO.
///
/// Session issuance, renewal, token verification and storage remain host-owned.
/// A renewal must preserve the host's original authentication time and absolute
/// lifetime. Supply its current effective expiry; this adapter never renews it.
pub trait AccessVerifier<E>: Send + Sync {
    fn verify<'a>(
        &'a self,
        evidence: &'a E,
        resource: &'a Resource,
        evaluation: &'a Evaluation,
    ) -> EvaluationFuture<'a, Option<AccessIdentity>>;
}

/// A resource decision that accepts host evidence instead of caller-supplied
/// principal/audience/credential facts. Owns no connection or authentication state.
pub struct AuthorizedAccess<E> {
    realm: String,
    audience: String,
    verifier: Arc<dyn AccessVerifier<E>>,
    authorization: Authorization,
}

impl<E: Send + Sync> AuthorizedAccess<E> {
    pub fn new(
        realm: impl Into<String>,
        audience: impl Into<String>,
        verifier: Arc<dyn AccessVerifier<E>>,
        authorization: Authorization,
    ) -> Result<Self, ConfigurationError> {
        let realm = realm.into();
        let audience = audience.into();
        if !identifier(&realm) || !identifier(&audience) {
            return Err(ConfigurationError);
        }
        Ok(Self {
            realm,
            audience,
            verifier,
            authorization,
        })
    }

    pub async fn check(
        &self,
        evidence: E,
        action: impl Into<String>,
        resource: Resource,
        context: Attributes,
    ) -> Decision {
        self.check_with_cancellation(evidence, action, resource, context, Cancellation::default())
            .await
    }

    pub async fn check_with_cancellation(
        &self,
        evidence: E,
        action: impl Into<String>,
        resource: Resource,
        context: Attributes,
        cancellation: Cancellation,
    ) -> Decision {
        let mut request = Request::new(None, action, resource);
        request.context = context;
        self.authorization
            .evaluate_with(
                request,
                cancellation,
                |mut request, evaluation| async move {
                    let Some(identity) = self
                        .verifier
                        .verify(&evidence, &request.resource, &evaluation)
                        .await?
                    else {
                        return Ok(None);
                    };
                    if identity.principal.realm_id != self.realm
                        || identity.audience != self.audience
                    {
                        return Ok(None);
                    }
                    request.principal = Some(identity.principal);
                    request.audience = Some(identity.audience);
                    request.credential = identity.credential;
                    Ok(Some((request, identity.session_expires_at)))
                },
            )
            .await
            .decision
    }

    pub async fn can(
        &self,
        evidence: E,
        action: impl Into<String>,
        resource: Resource,
        context: Attributes,
    ) -> bool {
        self.check(evidence, action, resource, context)
            .await
            .is_allowed()
    }

    pub async fn enforce(
        &self,
        evidence: E,
        action: impl Into<String>,
        resource: Resource,
        context: Attributes,
    ) -> Result<(), AuthorizationError> {
        if self.can(evidence, action, resource, context).await {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }

    pub async fn enforce_with_cancellation(
        &self,
        evidence: E,
        action: impl Into<String>,
        resource: Resource,
        context: Attributes,
        cancellation: Cancellation,
    ) -> Result<(), AuthorizationError> {
        if self
            .check_with_cancellation(evidence, action, resource, context, cancellation)
            .await
            .is_allowed()
        {
            Ok(())
        } else {
            Err(AuthorizationError)
        }
    }
}
