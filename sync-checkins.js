'use strict';

const config = require( 'config' );
const WooCommerceAPI = require( 'woocommerce-api' );
const UntappdClient = require( 'node-untappd' );
const querystring = require( 'querystring' );
const PromisePool = require( 'promise-pool-executor' );
const _ = require( 'lodash' );
const parseLinkHeader = require( 'parse-link-header' );

const WordPress = new WooCommerceAPI( {
    url: config.get( 'woocommerce.url' ),
    consumerKey: config.get( 'woocommerce.key' ),
    consumerSecret: config.get( 'woocommerce.secret' ),
    wpAPI: true,
    version: 'wp/v2',
} );

const untappdAccessTokens = config.get( 'untappd.tokens' );
let untappdAccessTokenIdx = 0;
let productUntappdIdMap = {};

const Untappd = new UntappdClient();

const rotateUntappdToken = () => {
    const nextIdx = untappdAccessTokenIdx++ % untappdAccessTokens.length;
    Untappd.setAccessToken( untappdAccessTokens[ nextIdx ] );
};

const getCheckins = ( args ) => new Promise( ( resolve, reject ) => {
    rotateUntappdToken();

    Untappd.pubFeed(
        ( err, response ) => {
            if ( err ) {
                reject( err );
            } else {
                if ( 'invalid_limit' == _.get( response, 'meta.error_type' ) ) {
                    console.error( '***** Hit Untappd Rate Limit *****' );
                    process.exit( 0 );
                }

                resolve( _.get( response, 'response.checkins.items', [] ) );
            }
        },
        Object.assign( {}, args, {
            limit: 25, radius: 25, dist_pref: 'm',
        } )
    );
} );

const getProductsWithUntappdId = ( page = 1 ) => new Promise( ( resolve, reject ) => {
    WordPress.get(
        'products?' + querystring.stringify( {
            per_page: 100,
            page,
            fields: [ 'id', 'title', 'meta.untappd_id' ],
        } ),
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( {
                        results: json,
                        totalPages: Number.parseInt( res.headers['x-wp-totalpages'] ),
                    } );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

const getAllProductsWithUntappdId = () => {
    return getProductsWithUntappdId().then( ( response ) => {
        if ( 1 == response.totalPages ) {
            return response.results;
        }

        const tasks = [ { results: response.results } ];

        for ( let i = 0; i < ( response.totalPages - 1 ); i++ ) {
            tasks.push( getProductsWithUntappdId( i + 2 ) );
        }

        return Promise.all( tasks ).then( ( resultSets ) => _.flatMap( resultSets, 'results' ) );
    } );
};

const storeCheckinAsPost = ( checkin ) => new Promise( ( resolve, reject ) => {
    if ( ! productUntappdIdMap[ checkin.beer.bid ] ) {
        return resolve();
    }

    const checkinData = {
        title: checkin.checkin_id + ' - ' + checkin.beer.beer_name,
        content: JSON.stringify( checkin ),
        meta_data: [
            {
                key: 'untappd_id',
                value: checkin.beer.bid,
            },
        ],
        parent: productUntappdIdMap[ checkin.beer.bid ].id,
        status: 'publish',
    };

    WordPress.post(
        'checkins',
        checkinData,
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( json );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

const storeCheckinsAsPosts = ( checkins ) => {
    // Create a pool with a concurrency limit
    const pool = new PromisePool.PromisePoolExecutor( {
        frequencyLimit: 10,
        frequencyWindow: 5000,
    } );

    return pool.addEachTask( {
        data: checkins,
        generator: storeCheckinAsPost,
    } ).promise().then( ( results ) => {
        console.log( results.length + ' checkins processed.' );
    } );
};

const backfillCheckins = ( lat, lng, min_id ) => {
    const processCheckins = ( checkins ) => {
        return storeCheckinsAsPosts( checkins ).then( () => {
            if ( 0 === checkins.length ) {
                return;
            }
            const yearInSeconds = 60 * 60 * 24 * 365;
            const dateDiff = new Date() - Date.parse( checkins[0].created_at );

            if ( min_id ) {
                if ( 25 === checkins.length ) {
                    return getCheckins( {
                        lat,
                        lng,
                        min_id: checkins[0].checkin_id,
                    } ).then( processCheckins );
                }

                return;
            }
            
            if ( dateDiff < yearInSeconds ) {
                return getCheckins( {
                    lat,
                    lng,
                    max_id: checkins[checkins.length - 1].checkin_id,
                } ).then( processCheckins );
            }
        } );
    };

    const initialArgs = { lat, lng };

    if ( min_id ) {
        initialArgs.min_id = min_id;
    }

    return getCheckins( initialArgs ).then( processCheckins );
};

const getLastStoredCheckin = () => new Promise( ( resolve, reject ) => {
    WordPress.get(
        'checkins?' + querystring.stringify( {
            per_page: 1,
            orderby: 'date',
            order: 'desc',
            context: 'edit',
        } ),
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( json.length ? JSON.parse( json[0].content.raw ) : false );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

getAllProductsWithUntappdId().then( ( products ) => {
    products.forEach( ( product ) => {
        const untappdId = _.get( product, 'meta.untappd_id' );

        if ( untappdId ) {
            productUntappdIdMap[ product.meta.untappd_id ] = {
                id: product.id,
                title: product.title.rendered,
            };
        }
    } );

    getLastStoredCheckin().then( ( checkin ) => {
        const lastCheckinId = checkin.checkin_id;

        backfillCheckins( 40.611763, -111.692505, lastCheckinId )                 // SLC, PC
        .then( () => backfillCheckins( 41.405450, -111.928711, lastCheckinId ) )  // Ogden
        .then( () => backfillCheckins( 37.326052, -113.532715, lastCheckinId ) )  // St George
        .then( () => backfillCheckins( 38.824303, -109.632568, lastCheckinId ) ); // Moab
    } );
} );
