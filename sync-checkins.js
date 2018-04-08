'use strict';

const config = require( 'config' );
const WooCommerceAPI = require( 'woocommerce-api' );
const UntappdClient = require( 'node-untappd' );
const querystring = require( 'querystring' );
const PromisePool = require( 'promise-pool-executor' );
const _ = require( 'lodash' );
const parseLinkHeader = require( 'parse-link-header' );
const utahUntappdVenues = require( './utah-untappd-venues.json' );

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

const getVenueCheckins = ( venueId, args ) => new Promise( ( resolve, reject ) => {
    rotateUntappdToken();

    Untappd.venueActivityFeed(
        ( err, response ) => {
            if ( err ) {
                reject( err );
            } else {
                const errorType = _.get( response, 'meta.error_type' );

                if ( 'invalid_limit' == errorType ) {
                    console.error( '***** Hit Untappd Rate Limit *****' );
                    process.exit( 0 );
                }

                if ( 'invalid_param' == errorType ) {
                    console.error( _.get( response, 'meta.error_detail' ) );
                    process.exit( 0 );
                }

                resolve( _.get( response, 'response.checkins.items', [] ) );
            }
        },
        Object.assign( {}, args, {
            VENUE_ID: venueId,
            limit: 25,
        } )
    );
} );

const getProductsWithUntappdId = ( page = 1 ) => new Promise( ( resolve, reject ) => {
    WordPress.get(
        'products?' + querystring.stringify( {
            per_page: 100,
            page,
            fields: [ 'id', 'title', 'meta.untappd_id' ].join( ',' ),
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
        console.log( `Skipping checkin ${checkin.checkin_id} - no matching product.` );
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
            {
                key: 'untappd_checkin_id',
                value: checkin.checkin_id,
            },
        ],
        parent: productUntappdIdMap[ checkin.beer.bid ].id,
        status: 'publish',
    };

    console.log( `Storing checkin ${checkin.checkin_id} as post.` );

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

const backfillVenueCheckins = ( venueId, min_id ) => {
    console.log( 'backfilling venue checkins', venueId );
    const processVenueCheckins = ( checkins ) => {
        return storeCheckinsAsPosts( checkins ).then( () => {
            if ( 0 === checkins.length ) {
                return;
            }

            const oneYearInSeconds = 60 * 60 * 24 * 365;
            const dateDiff = new Date() - Date.parse( checkins[0].created_at );

            if ( 25 > checkins.length ) {
                return;
            }

            if ( min_id ) {
                return getVenueCheckins( venueId, {
                    min_id: checkins[0].checkin_id,
                } ).then( processVenueCheckins );
            }
            
            if ( dateDiff < oneYearInSeconds ) {
                return getVenueCheckins( venueId, {
                    max_id: checkins[checkins.length - 1].checkin_id,
                } ).then( processVenueCheckins );
            }
        } );
    };

    const initialArgs = {};

    if ( min_id ) {
        initialArgs.min_id = min_id;
    }

    return getVenueCheckins( venueId, initialArgs ).then( processVenueCheckins );
};

const processAllVenues = ( min_checkin_id ) => {
    console.log( 'processing venues, min id: ', min_checkin_id );
    const pool = new PromisePool.PromisePoolExecutor( {
        frequencyLimit: 1,
        frequencyWindow: 2000,
    } );

    return pool.addEachTask( {
        data: utahUntappdVenues,
        generator: ( venue ) => backfillVenueCheckins( venue.venue_id, min_checkin_id ),
    } ).promise().then( ( results ) => {
        console.log( results.length + ' venue checkins processed.' );
    } );
};

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

    getLastStoredCheckin().then( ( checkin ) => processAllVenues( checkin.checkin_id ) );
} );
